"""Tests for BlockingChannel's per-RPC deadline (`rpc_timeout`).

These tests drive a real `pika.channel.Channel` and a real
`CallbackManager` behind the `BlockingChannel`, with a fake transport that
captures outbound methods, delivers scheduled inbound replies, and advances
a fake clock, so that reply ordering and deadline expiry are deterministic.
"""

import collections
import unittest
from unittest import mock

import pika._utils
from pika import callback, channel, exceptions, frame, spec
from pika.adapters import blocking_connection

_CHANNEL_NUMBER = 1


class _FakeClock:
    """Monotonic clock the test advances explicitly."""

    def __init__(self, now: float = 1000.0) -> None:
        self.now = now

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class _FakeBroker:
    """Stands in for both the connection impl and the broker.

    Towards `pika.channel.Channel` it is the connection: it owns the shared
    `CallbackManager` and captures every method the client sends. Towards
    `BlockingChannel` it is the `BlockingConnection`: its `_flush_output`
    delivers due replies and lets time pass until a waiter is ready or one
    raises, mirroring how `BlockingConnection._flush_output` evaluates its
    waiters between I/O passes.
    """

    # Safety net against a wait that can never end; well above any fake
    # clock advance these tests schedule.
    _MAX_FLUSH_ITERATIONS = 2_000_000

    def __init__(self, clock: _FakeClock) -> None:
        self._clock = clock
        self.callbacks = callback.CallbackManager()
        self.sent_methods = []
        # Replies the "broker" will send, as (deliver_at, method) in order.
        self._inbox = collections.deque()
        # Members BlockingChannel touches on its BlockingConnection.
        self._server_channel_closures = []
        self._dispatch_requests = []

    def schedule(self, deliver_at: float, method) -> None:
        """Queue a broker reply to be delivered at the given fake time."""
        self._inbox.append((deliver_at, method))

    def _send_method(self, channel_number, method, content=None):
        """`pika.channel.Channel` outbound hook; captures sent methods."""
        self.sent_methods.append(method)

    def _marshal_message(self, channel_number, method_frame, content):
        """Captures published messages; the bytes themselves are not needed."""
        self.sent_methods.append(method_frame)
        return []

    def _output_marshaled_frames(self, marshaled_frames):
        pass

    def _request_channel_dispatch(self, channel_number):
        self._dispatch_requests.append(channel_number)

    def _flush_output(self, *waiters):
        for _ in range(self._MAX_FLUSH_ITERATIONS):
            self._deliver_due()
            if any(waiter() for waiter in waiters):
                return
            self._clock.advance(1)
        raise AssertionError('waiters never became ready')

    def _deliver_due(self):
        while self._inbox and self._inbox[0][0] <= self._clock.now:
            _, method = self._inbox.popleft()
            self.callbacks.process(_CHANNEL_NUMBER, method, self,
                                   frame.Method(_CHANNEL_NUMBER, method))


class BlockingChannelRpcTimeoutTests(unittest.TestCase):

    def setUp(self):
        self.clock = _FakeClock()
        self.broker = _FakeBroker(self.clock)
        self.channel_impl = channel.Channel(self.broker, _CHANNEL_NUMBER,
                                            mock.Mock())
        self.channel_impl._on_openok(
            frame.Method(_CHANNEL_NUMBER, spec.Channel.OpenOk()))
        self.obj = blocking_connection.BlockingChannel(self.channel_impl,
                                                       self.broker)
        patcher = mock.patch.object(pika._utils, 'time_now', self.clock.time)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self):
        del self.broker
        del self.channel_impl
        del self.obj

    def sent_method_classes(self):
        return [type(m) for m in self.broker.sent_methods]

    @staticmethod
    def declare_ok(message_count):
        return spec.Queue.DeclareOk(queue='q',
                                    message_count=message_count,
                                    consumer_count=0)

    def test_reply_before_deadline_completes_normally(self):
        reply = self.declare_ok(message_count=1)
        self.broker.schedule(self.clock.now + 5, reply)

        result = self.obj.queue_declare('q', rpc_timeout=10)

        self.assertIs(result.method, reply)
        self.assertEqual(self.sent_method_classes(), [spec.Queue.Declare])

    def test_sent_rpc_timeout_reports_maybe_applied(self):
        with self.assertRaises(exceptions.SynchronousRpcTimeout) as ctx:
            self.obj.queue_declare('q', rpc_timeout=10)

        self.assertTrue(ctx.exception.maybe_applied)
        self.assertEqual(self.sent_method_classes(), [spec.Queue.Declare])
        # The impl channel still considers the RPC outstanding: its reply,
        # if it ever arrives, is paired with this request.
        self.assertEqual(self.channel_impl._blocking, 'Queue.Declare')

    def test_late_reply_is_paired_with_timed_out_rpc_not_the_next_one(self):
        # First declare is sent but its reply never arrives in time.
        with self.assertRaises(exceptions.SynchronousRpcTimeout):
            self.obj.queue_declare('q', rpc_timeout=10)
        self.assertEqual(self.sent_method_classes(), [spec.Queue.Declare])

        # Asynchronous methods still go out while the synchronous RPC is
        # outstanding.
        self.obj.basic_publish('ex', 'rk', b'body')
        self.assertEqual(self.sent_method_classes(),
                         [spec.Queue.Declare, spec.Basic.Publish])

        # The identical RPC queues behind the timed-out one instead of going
        # on the wire; the late reply then unblocks it, and only the second
        # DeclareOk may complete it.
        late_reply = self.declare_ok(message_count=1)
        second_reply = self.declare_ok(message_count=2)
        self.broker.schedule(self.clock.now + 1, late_reply)
        self.broker.schedule(self.clock.now + 2, second_reply)

        result = self.obj.queue_declare('q', rpc_timeout=10)

        self.assertIs(result.method, second_reply)
        self.assertEqual(self.sent_method_classes(),
                         [spec.Queue.Declare, spec.Basic.Publish,
                          spec.Queue.Declare])

        # A different RPC works afterwards.
        delete_reply = spec.Queue.DeleteOk(message_count=0)
        self.broker.schedule(self.clock.now + 1, delete_reply)
        delete_result = self.obj.queue_delete('q', rpc_timeout=10)
        self.assertIs(delete_result.method, delete_reply)
        self.assertEqual(self.sent_method_classes(),
                         [spec.Queue.Declare, spec.Basic.Publish,
                          spec.Queue.Declare, spec.Queue.Delete])

    def test_queued_rpc_timeout_is_withdrawn_without_being_sent(self):
        with self.assertRaises(exceptions.SynchronousRpcTimeout) as first:
            self.obj.queue_declare('q', rpc_timeout=10)
        self.assertTrue(first.exception.maybe_applied)

        # No reply is coming this time either, so the second declare expires
        # while still queued behind the first: it is withdrawn unsent.
        with self.assertRaises(exceptions.SynchronousRpcTimeout) as second:
            self.obj.queue_declare('q', rpc_timeout=10)
        self.assertFalse(second.exception.maybe_applied)
        self.assertEqual(self.sent_method_classes(), [spec.Queue.Declare])
        self.assertEqual(len(self.channel_impl._blocked), 0)

        # When the first declare's late reply arrives, the channel unblocks
        # and a queued RPC goes out and completes.
        self.broker.schedule(self.clock.now + 1, self.declare_ok(0))
        delete_reply = spec.Queue.DeleteOk(message_count=0)
        self.broker.schedule(self.clock.now + 2, delete_reply)

        result = self.obj.queue_delete('q', rpc_timeout=10)

        self.assertIs(result.method, delete_reply)
        self.assertEqual(self.sent_method_classes(),
                         [spec.Queue.Declare, spec.Queue.Delete])

    def test_reply_processed_at_the_deadline_wins_over_the_timeout(self):
        reply = self.declare_ok(message_count=1)
        self.broker.schedule(self.clock.now + 10, reply)

        result = self.obj.queue_declare('q', rpc_timeout=10)

        self.assertIs(result.method, reply)

    def test_sent_basic_qos_timeout_leaves_channel_usable(self):
        with self.assertRaises(exceptions.SynchronousRpcTimeout) as ctx:
            self.obj.basic_qos(prefetch_count=1, rpc_timeout=10)
        self.assertTrue(ctx.exception.maybe_applied)
        self.assertEqual(self.sent_method_classes(), [spec.Basic.Qos])

        # The late QosOk unblocks the channel; the next RPC then completes
        # with its own reply.
        self.broker.schedule(self.clock.now + 1, spec.Basic.QosOk())
        reply = self.declare_ok(message_count=0)
        self.broker.schedule(self.clock.now + 2, reply)

        result = self.obj.queue_declare('q', rpc_timeout=10)

        self.assertIs(result.method, reply)
        self.assertEqual(self.sent_method_classes(),
                         [spec.Basic.Qos, spec.Queue.Declare])

    def test_queued_basic_qos_timeout_is_withdrawn_without_being_sent(self):
        with self.assertRaises(exceptions.SynchronousRpcTimeout):
            self.obj.basic_qos(prefetch_count=1, rpc_timeout=10)

        with self.assertRaises(exceptions.SynchronousRpcTimeout) as ctx:
            self.obj.basic_qos(prefetch_count=2, rpc_timeout=10)

        self.assertFalse(ctx.exception.maybe_applied)
        self.assertEqual(self.sent_method_classes(), [spec.Basic.Qos])
        self.assertEqual(len(self.channel_impl._blocked), 0)

    def test_without_rpc_timeout_waits_indefinitely(self):
        reply = self.declare_ok(message_count=1)
        self.broker.schedule(self.clock.now + 100_000, reply)

        result = self.obj.queue_declare('q')

        self.assertIs(result.method, reply)

    def test_negative_rpc_timeout_is_rejected(self):
        with self.assertRaises(ValueError):
            self.obj.queue_declare('q', rpc_timeout=-1)

    def test_channel_close_after_timeout_cleans_up_callbacks(self):
        with self.assertRaises(exceptions.SynchronousRpcTimeout):
            self.obj.queue_declare('q', rpc_timeout=10)

        self.channel_impl._on_close_from_broker(
            frame.Method(_CHANNEL_NUMBER, spec.Channel.Close(404, 'no q')))

        self.assertTrue(self.obj.is_closed)
        # The orphaned-reply sink and the impl's synchronous-completion
        # callback are gone with the channel.
        self.assertNotIn(str(_CHANNEL_NUMBER), self.broker.callbacks._stack)
        with self.assertRaises(exceptions.ChannelWrongStateError):
            self.obj.queue_declare('q', rpc_timeout=10)


if __name__ == '__main__':
    unittest.main()
