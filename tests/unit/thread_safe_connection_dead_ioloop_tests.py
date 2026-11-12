"""Deterministic repros for waits that never complete after a failed I/O thread.

Each case builds the failure with a fake IOLoop or a killed pool worker rather than a live broker,
and asserts the wait returns inside a short bound.  A regression shows up as the observation event
never being set, reported as an explicit failure instead of a stalled run.  The six waits that
currently hang are marked expectedFailure; fixing one turns its test into an unexpected success,
which fails the suite and is the signal to drop the marker.
"""

import contextlib
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from pika.adapters.thread_safe_connection import (
    DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
    Channel,
    Connection,
    _BoundedWorkPool,
)

# Long enough for a correct implementation to return, far short of any budget under test.
_BOUND = 2.5

STANDALONE = type('StandaloneOwner', (), {})()


class _Killed(BaseException):
    """Stands in for a BaseException that ends the IOLoop thread."""


def _raise_killed():
    raise _Killed('I/O thread ended on a BaseException')


def _run_bounded(target):
    """Run *target* on a daemon thread and return whether it finished within _BOUND."""
    finished = threading.Event()

    def body():
        try:
            target()
        finally:
            finished.set()

    thread = threading.Thread(target=body, daemon=True)
    thread.start()
    finished.wait(timeout=_BOUND)
    thread.join(timeout=0.1)
    return not thread.is_alive()


def _make_connection_over_ioloop(ioloop_start=None):
    """Build a Connection over a mocked SelectConnection, keeping its pools usable.

    The mock fires on_open_callback so __init__ completes and holds ioloop.start() open, so the
    IOLoop thread has not run its pool-shutdown tail.  Cleanup releases the loop and joins every
    pool so a stray worker cannot outlive the test.
    """
    running = threading.Event()
    with patch('pika.adapters.thread_safe_connection.SelectConnection') as MockSelectConn:
        mock_conn = MagicMock()
        mock_ioloop = MagicMock()
        mock_conn.ioloop = mock_ioloop
        mock_conn.is_open = True
        mock_conn.is_closed = False
        mock_conn.is_closing = False
        mock_conn._error = None

        def fake_init(parameters, on_open_callback, on_open_error_callback, on_close_callback):
            on_open_callback(mock_conn)
            return mock_conn

        MockSelectConn.side_effect = fake_init
        MockSelectConn.return_value = mock_conn
        mock_ioloop.start.side_effect = ioloop_start or (lambda: running.wait(timeout=30))
        conn = Connection(parameters='params')

    def release():
        with conn._channel_waiters_lock:
            pools = [
                p for p in (getattr(ch, '_consumer_work_pool', None)
                            for ch in conn._channels) if p is not None
            ]
        pools.append(conn._connection_work_pool)
        for pool in pools:
            pool.shutdown(wait=False)
        for pool in pools:
            pool.shutdown(wait=True, timeout=5)
        running.set()
        conn._ioloop_thread.join(timeout=10)

    conn._release_harness = release
    return conn, mock_conn, mock_ioloop


class FailedIOLoopWaits(unittest.TestCase):
    """Blocking calls whose only wake-up source is the IOLoop thread."""

    def _release_waiters(self, wrapper):
        """Wake every waiter a repro left blocked, so no daemon thread outlives the test.

        The call under test never returns on its own, which is the defect; releasing the event
        lets the thread unwind after the assertion has recorded that.
        """
        with wrapper._channel_waiters_lock:
            waiters = list(wrapper._blocking_waiters)
            wrapper._blocking_waiters.clear()
        for event, error in waiters:
            if error[0] is None:
                error[0] = _Killed('test over')
            event.set()

    def _make_channel(self):
        """A Channel whose scheduled work is dropped after the IOLoop thread dies."""
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        raw_ch.is_closing = False
        raw_ch._consumers = {}
        raw_ch._cancelled = set()
        wrapper = MagicMock()
        wrapper._closed_reason = None
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        wrapper._connection._error = None
        wrapper._ioloop_thread.is_alive.return_value = False
        wrapper._schedule_unchecked.side_effect = lambda cb: None  # scheduler is gone
        return Channel(raw_ch, wrapper), raw_ch, wrapper

    @unittest.expectedFailure  # ready.wait() at thread_safe_connection.py:915 has no liveness check
    def test_blocking_rpc_with_no_timeout_hangs_when_ioloop_thread_dies(self):
        """queue_declare(timeout=None) waits on a response only the IOLoop could deliver."""
        ch, _raw_ch, wrapper = self._make_channel()
        self.addCleanup(self._release_waiters, wrapper)
        outcome = []

        def call():
            try:
                ch.queue_declare(queue='q', timeout=None)
            except BaseException as exc:  # noqa: BLE001 - recorded for the assertion
                outcome.append(exc)

        returned = _run_bounded(call)

        self.assertTrue(
            returned,
            'queue_declare(timeout=None) never returned after the IOLoop thread died: '
            'nothing sets _closed_reason or its Event, so the wait has no liveness '
            'condition and no deadline')
        self.assertTrue(outcome, 'the call returned without an exception')

    @unittest.expectedFailure  # ready.wait() at thread_safe_connection.py:1832, known gap
    def test_channel_close_with_no_timeout_hangs_when_ioloop_thread_dies(self):
        """Channel.close(timeout=None) waits for Channel.CloseOk from the dead thread."""
        ch, _raw_ch, wrapper = self._make_channel()
        self.addCleanup(self._release_waiters, wrapper)
        outcome = []

        def call():
            try:
                ch.close(timeout=None)
            except BaseException as exc:  # noqa: BLE001
                outcome.append(exc)

        returned = _run_bounded(call)

        self.assertTrue(
            returned,
            'Channel.close(timeout=None) never returned: its ready.wait has no liveness '
            'check, which docs/internal/thread-safe-connection.md records as the one '
            'unbounded wait in the close path')
        self.assertTrue(outcome, 'the call returned without an exception')

    @unittest.expectedFailure  # ready.wait() at thread_safe_connection.py:1153, same gap
    def test_basic_get_with_no_timeout_hangs_when_ioloop_thread_dies(self):
        """basic_get(timeout=None) has the same structure and the same gap."""
        ch, _raw_ch, wrapper = self._make_channel()
        self.addCleanup(self._release_waiters, wrapper)

        def call():
            ch.basic_get(queue='q', timeout=None)

        returned = _run_bounded(call)
        self.assertTrue(
            returned,
            'basic_get(timeout=None) never returned after the IOLoop thread died')

    @unittest.expectedFailure  # ready.wait() at thread_safe_connection.py:2290, same gap
    def test_channel_open_with_no_timeout_hangs_when_ioloop_thread_dies(self):
        """Connection.channel(timeout=None) registers a waiter nothing will wake."""
        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop(
            ioloop_start=_raise_killed)
        self.addCleanup(conn._release_harness)
        conn._ioloop_thread.join(timeout=5)
        self.assertFalse(conn._ioloop_thread.is_alive())
        self.addCleanup(self._release_waiters, conn)
        outcome = []

        def call():
            try:
                conn.channel(timeout=None)
            except BaseException as exc:  # noqa: BLE001
                outcome.append(exc)

        returned = _run_bounded(call)
        self.assertTrue(
            returned,
            'Connection.channel(timeout=None) never returned after the IOLoop thread died')

    def test_connection_close_with_no_timeout_does_not_block_on_a_dead_thread(self):
        """Connection.close(timeout=None) joins a thread that has already exited."""
        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop(
            ioloop_start=_raise_killed)
        self.addCleanup(conn._release_harness)
        conn._ioloop_thread.join(timeout=5)
        started = time.monotonic()

        def call():
            conn.close(timeout=None)

        returned = _run_bounded(call)
        elapsed = time.monotonic() - started
        self.assertTrue(returned,
                        f'Connection.close(timeout=None) hung for {elapsed:.1f}s on an '
                        'already-dead IOLoop thread')

    @unittest.expectedFailure  # _connected_event.wait() at thread_safe_connection.py:2088
    def test_constructor_with_no_timeout_hangs_when_ioloop_dies_before_open(self):
        """Connection(timeout=None) waits for an open that can no longer arrive.

        The only code that wakes this wait is inside _run_ioloop's `except Exception`; a
        BaseException skips it, and the wait itself has no liveness check.  The failed
        constructor never returns its object, so its daemon thread stays blocked; that thread
        is the evidence and is left for the interpreter to discard.
        """
        pools = []

        def capturing_pool(*args, **kwargs):
            pool = _BoundedWorkPool(*args, **kwargs)
            pools.append(pool)
            return pool

        running = threading.Event()
        with contextlib.ExitStack() as stack:
            MockSelectConn = stack.enter_context(
                patch('pika.adapters.thread_safe_connection.SelectConnection'))
            stack.enter_context(
                patch('pika.adapters.thread_safe_connection._BoundedWorkPool',
                      side_effect=capturing_pool))
            mock_conn = MagicMock()
            mock_ioloop = MagicMock()
            mock_conn.ioloop = mock_ioloop
            mock_conn.is_open = False
            mock_conn.is_closed = False
            mock_conn._error = None
            # on_open_callback never fires: the loop dies before the connection opens.
            MockSelectConn.side_effect = lambda **kw: mock_conn
            MockSelectConn.return_value = mock_conn

            def ioloop_start():
                running.wait(timeout=5)
                _raise_killed()

            mock_ioloop.start.side_effect = ioloop_start

            def construct():
                Connection(parameters='params', timeout=None)

            returned = _run_bounded(construct)
            try:
                self.assertTrue(
                    returned,
                    'Connection(timeout=None) never returned after the IOLoop thread died '
                    'before opening: _connected_event.wait() has no liveness condition and '
                    'the crash tail that sets it only runs for Exception')
            finally:
                running.set()
                for pool in pools:
                    pool.shutdown(wait=True, timeout=5)

    @unittest.expectedFailure  # force-close branch at thread_safe_connection.py:2434 is skipped
    def test_close_after_ioloop_death_records_a_reason_and_wakes_waiters(self):
        """A later Connection.close() must not leave waiters stranded on a dead thread.

        The force-close path that records a reason and wakes waiters runs only when the
        IOLoop thread is still alive; after a BaseException death it is skipped, so a
        waiter blocked in an RPC is never released even by an explicit close().
        """
        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop(
            ioloop_start=_raise_killed)
        self.addCleanup(conn._release_harness)
        conn._ioloop_thread.join(timeout=5)
        self.assertFalse(conn._ioloop_thread.is_alive())
        ready = threading.Event()
        error = [None]
        with conn._channel_waiters_lock:
            conn._blocking_waiters.append((ready, error))

        conn.close(timeout=1)

        self.assertIsNotNone(
            conn._closed_reason,
            'close() returned without recording a reason, so every blocked waiter '
            'stays asleep on a thread that no longer exists')
        self.assertTrue(ready.is_set(),
                        'close() did not wake the waiter registered before it')

    def test_poisoned_ioloop_records_no_close_reason(self):
        """A BaseException leaves _closed_reason unset, which is why nothing wakes the waiter."""
        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop(
            ioloop_start=_raise_killed)
        self.addCleanup(conn._release_harness)
        conn._ioloop_thread.join(timeout=5)
        self.assertFalse(conn._ioloop_thread.is_alive())
        self.assertIsNone(
            conn._closed_reason,
            '_closed_reason was set, so _run_ioloop caught the crash and this test no '
            'longer exercises the BaseException path')
        self.assertEqual(conn._blocking_waiters, [])


class KilledWorkerWaits(unittest.TestCase):
    """Wait sites that depend on a pool worker, and are safe or unsafe accordingly."""

    def _kill_worker(self, pool):
        """End the worker deterministically with a malformed queue item.

        The per-item guard catches BaseException, but unpacking the item happens above it, so one
        bad item ends the worker.  No sleep is needed: the caller polls is_alive with a deadline.
        """
        pool.submit(lambda: None)
        pool._queue.put(object())
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if pool._thread is not None and not pool._thread.is_alive():
                return
            time.sleep(0.01)
        self.fail('the worker survived the malformed item')

    def test_shutdown_reports_success_on_a_dead_worker_and_orphans_the_queue(self):
        """A dead worker makes join return at once, so shutdown reports success with no warning.

        Unlike drain, shutdown cannot tell a worker that drained from one that died mid-item:
        both answer is_alive() False.  The work queued behind the fatal item is therefore
        abandoned silently rather than reported, which is the wrong-answer case this pair covers.
        """
        pool = _BoundedWorkPool(maxsize=10, put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool', owner=STANDALONE)
        self._kill_worker(pool)
        orphan_ran = threading.Event()
        pool._queue.put((orphan_ran.set, ()))
        started = time.monotonic()
        result = pool.shutdown(wait=True, timeout=None)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0,
                        'joining an already-dead worker must return at once')
        self.assertTrue(
            result,
            'shutdown answers success for a dead worker, so _shutdown_pool logs no '
            'abandonment warning; the orphaned item is what distinguishes this from a drain')
        self.assertFalse(
            orphan_ran.wait(timeout=0.2),
            'the item behind the fatal worker ran, so it was not actually orphaned')

    def test_shutdown_with_no_timeout_hangs_on_a_wedged_worker(self):
        """The genuine unbounded wait is a wedged (still alive) worker, not a dead one."""
        pool = _BoundedWorkPool(maxsize=1, put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool', owner=STANDALONE)
        release = threading.Event()
        started_cb = threading.Event()

        def wedged():
            started_cb.set()
            release.wait(timeout=10)

        pool.submit(wedged)
        self.assertTrue(started_cb.wait(timeout=5))
        try:
            returned = _run_bounded(lambda: pool.shutdown(wait=True, timeout=None))
            self.assertFalse(
                returned,
                'shutdown(timeout=None) returned despite a worker stuck in a callback; '
                'this is the slow-callback hang a finite close budget is meant to avoid')
        finally:
            release.set()
            pool.shutdown(wait=True, timeout=5)

    def test_drain_returns_false_when_the_worker_dies(self):
        """drain already rechecks liveness; it is the model the other waits lack."""
        pool = _BoundedWorkPool(maxsize=1, put_timeout=DEFAULT_WORK_QUEUE_PUT_TIMEOUT,
                                thread_name='test-pool', owner=STANDALONE)
        self._kill_worker(pool)
        started = time.monotonic()
        result = pool.drain(timeout=None)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0)
        self.assertFalse(result,
                         'drain should report False for a dead worker rather than hang')


class SlowCallbackControls(unittest.TestCase):
    """A slow but alive thread is bounded by the caller's budget; only a lost thread hangs."""

    def test_blocking_rpc_with_finite_timeout_returns_after_its_budget(self):
        """A short RPC timeout is a correct bound even with no reply."""
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        wrapper = MagicMock()
        wrapper._closed_reason = None
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        wrapper._connection._error = None
        # The IOLoop is alive but never runs the scheduled work.
        wrapper._ioloop_thread.is_alive.return_value = True
        wrapper._schedule_unchecked.side_effect = lambda cb: None
        ch = Channel(raw_ch, wrapper)

        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            ch.queue_declare(queue='q', timeout=0.1)
        self.assertLess(time.monotonic() - started, 1.0)

    def test_channel_close_with_finite_timeout_returns_even_without_closeok(self):
        """Channel.close(timeout=...) spends its budget and returns instead of hanging.

        With no CloseOk the pool is deliberately left for the connection sweep rather than shut
        down, so the assertion is only that the caller gets its thread back within the budget.
        """
        raw_ch = MagicMock()
        raw_ch.channel_number = 1
        raw_ch.is_open = True
        raw_ch.is_closed = False
        raw_ch.is_closing = False
        raw_ch._consumers = {}
        raw_ch._cancelled = set()
        wrapper = MagicMock()
        wrapper._closed_reason = None
        wrapper._closed_reason_tb = None
        wrapper._channel_waiters_lock = threading.Lock()
        wrapper._blocking_waiters = []
        wrapper._connection._error = None
        wrapper._ioloop_thread.is_alive.return_value = True
        wrapper._schedule_unchecked.side_effect = lambda cb: cb()
        ch = Channel(raw_ch, wrapper)

        started = time.monotonic()
        ch.close(timeout=0.2)
        self.assertLess(time.monotonic() - started, 1.5)

    def test_connection_close_with_no_timeout_hangs_on_a_wedged_ioloop_thread(self):
        """The join-based hang needs an alive thread; a dead one returns at once.

        Connection.close(timeout=None) joins _ioloop_thread without a liveness recheck.  This is
        the slow-callback counterpart of the dead-thread cases above and the reason a finite close
        timeout exists.
        """
        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop()
        self.addCleanup(conn._release_harness)
        # The harness holds ioloop.start() open on a live thread, so the join blocks.
        self.assertTrue(conn._ioloop_thread.is_alive())
        returned = _run_bounded(lambda: conn.close(timeout=None))
        self.assertFalse(
            returned,
            'Connection.close(timeout=None) returned while the IOLoop thread was still '
            'running; the unbounded join is the slow-callback hang')

    def test_exception_crash_records_a_close_reason(self):
        """An Exception is caught by _run_ioloop, so waiters registered before it get woken.

        This is the contrast with the BaseException cases in FailedIOLoopWaits: those leave
        _closed_reason None, which is precisely why an indefinite wait there has no answer.
        """
        crash = RuntimeError('ordinary callback crash')

        def ioloop_start():
            raise crash

        conn, _mock_conn, _mock_ioloop = _make_connection_over_ioloop(ioloop_start=ioloop_start)
        self.addCleanup(conn._release_harness)
        conn._ioloop_thread.join(timeout=5)
        self.assertIs(
            conn._closed_reason, crash,
            'an Exception crash must record a close reason; only BaseException bypasses '
            'the _run_ioloop guard and leaves waiters without a wake-up')


if __name__ == '__main__':
    unittest.main()
