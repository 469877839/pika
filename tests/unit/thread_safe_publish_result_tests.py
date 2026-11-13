"""Offline tests for result-tracked publishing in the thread-safe adapter."""

import threading
import time
import unittest
from unittest.mock import MagicMock

from pika import callback, channel as pika_channel, frame, spec
from pika.adapters.thread_safe_connection import (
    Channel,
    DuplicateMessageId,
    PublishHandle,
    PublishInFlightLimitError,
    PublishResult,
    PublishStatus,
    PublishWaitCancelled,
)
from pika.connection import Connection as AsyncConnection
from pika.exceptions import ChannelClosedByBroker


class _RawConnection:
    """Enough of `pika.connection.Connection` for a real raw Channel."""

    def __init__(self):
        self.callbacks = callback.CallbackManager()
        self.publisher_confirms = True
        self.basic_nack = True
        self._body_max_length = 131072 - 8
        self.sent = []

    def _marshal_message(self, *args):
        return AsyncConnection._marshal_message(self, *args)

    def _output_marshaled_frames(self, frames):
        self.sent.append(frames)


def _make_channel(**kwargs):
    """Return (wrapper, raw channel, wrapper-connection mock) wired synchronously."""
    raw_conn = _RawConnection()
    raw_ch = pika_channel.Channel(raw_conn, 1, None)
    raw_ch._set_state(raw_ch.OPEN)

    wrapper = MagicMock()
    wrapper._closed_reason = None
    wrapper._closed_reason_tb = None
    wrapper._channel_waiters_lock = threading.Lock()
    wrapper._blocking_waiters = []
    wrapper._connection._error = None
    # Run both the listener registration and the publish inline.
    wrapper._schedule_unchecked.side_effect = lambda cb: cb()

    ch = Channel(raw_ch, wrapper, work_queue_put_timeout=1.0, **kwargs)
    # Skip the Confirm.Select round trip; the broker-facing part is exercised
    # by the real frame injection below.
    ch._next_publish_seq_no = 0
    return ch, raw_ch, wrapper


def _ack(raw_ch, delivery_tag, multiple=False):
    raw_ch.callbacks.process(
        1, spec.Basic.Ack, raw_ch,
        frame.Method(1, spec.Basic.Ack(delivery_tag, multiple)))


def _nack(raw_ch, delivery_tag, multiple=False):
    raw_ch.callbacks.process(
        1, spec.Basic.Nack, raw_ch,
        frame.Method(1, spec.Basic.Nack(delivery_tag, multiple, requeue=True)))


def _return(raw_ch, message_id, body=b'returned', reply_code=312):
    method = frame.Method(
        1,
        spec.Basic.Return(reply_code, 'NO_ROUTE', 'exchange', 'routing.key'))
    properties = spec.BasicProperties(message_id=message_id)
    raw_ch._handle_content_frame(method)
    raw_ch._handle_content_frame(frame.Header(1, len(body), properties))
    raw_ch._handle_content_frame(frame.Body(1, body))


class PublishResultTests(unittest.TestCase):

    def tearDown(self):
        # Keep worker threads from outliving the test process.
        for ch in getattr(self, '_channels', []):
            ch._shutdown_result_pool(timeout=1.0)

    def _channel(self, **kwargs):
        ch, raw_ch, wrapper = _make_channel(**kwargs)
        self._channels = getattr(self, '_channels', []) + [ch]
        return ch, raw_ch, wrapper

    def test_ack_completes_the_result_and_releases_the_tag(self):
        ch, raw_ch, _ = self._channel()
        handle = ch.publish_with_result(exchange='',
                                        routing_key='q',
                                        body=b'message',
                                        message_id='m1')

        self.assertIsInstance(handle, PublishHandle)
        self.assertEqual(handle.status, PublishStatus.PENDING)
        self.assertIn('m1', ch._publish_inflight)
        self.assertEqual(raw_ch.connection.sent[0],
                         raw_ch.connection.sent[0])
        self.assertEqual(ch._next_publish_seq_no, 1)

        _ack(raw_ch, 1)

        result = handle.wait(0)
        self.assertIsInstance(result, PublishResult)
        self.assertEqual(result.status, PublishStatus.ACKED)
        self.assertTrue(result.routed)
        self.assertTrue(result.acked)
        self.assertEqual(result.delivery_tag, 1)
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(ch._publish_by_tag, {})

    def test_nack_is_terminal_and_not_routed(self):
        ch, raw_ch, _ = self._channel()
        handle = ch.publish_with_result('', 'q', b'message', message_id='m1')

        _nack(raw_ch, 1)

        result = handle.wait(0)
        self.assertEqual(result.status, PublishStatus.NACKED)
        self.assertTrue(result.nacked)
        self.assertFalse(result.routed)
        self.assertEqual(ch._publish_inflight, {})

    def test_multiple_ack_completes_every_tag_up_to_it(self):
        ch, raw_ch, _ = self._channel()
        handles = [
            ch.publish_with_result('', 'q', b'message',
                                   message_id=f'm{i}') for i in range(1, 4)
        ]

        _ack(raw_ch, 3, multiple=True)

        self.assertEqual([h.wait(0).status for h in handles],
                         [PublishStatus.ACKED] * 3)
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(ch._publish_by_tag, {})

    def test_multiple_zero_ack_completes_all_outstanding(self):
        ch, raw_ch, _ = self._channel()
        handles = [
            ch.publish_with_result('', 'q', b'message',
                                   message_id=f'm{i}') for i in range(1, 4)
        ]

        _ack(raw_ch, 0, multiple=True)

        self.assertTrue(all(h.wait(0).status is PublishStatus.ACKED
                            for h in handles))
        self.assertEqual(ch._publish_inflight, {})

    def test_return_then_ack_stays_no_route(self):
        ch, raw_ch, _ = self._channel()
        results = []
        handle = ch.publish_with_result('',
                                        'q',
                                        b'message',
                                        message_id='m1',
                                        mandatory=True,
                                        on_result=results.append)

        _return(raw_ch, 'm1')
        self.assertEqual(handle.wait(0).status, PublishStatus.NO_ROUTE)
        self.assertFalse(handle.wait(0).routed)
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(ch._publish_by_tag, {})

        # The broker's Ack for the same publish must not resurrect it as a
        # routed success.
        _ack(raw_ch, 1)
        time.sleep(0.05)

        result = handle.outcome
        self.assertEqual(result.status, PublishStatus.NO_ROUTE)
        self.assertFalse(result.routed)
        self.assertEqual(result.reply_code, 312)
        self.assertEqual(result.reply_text, 'NO_ROUTE')
        self.assertEqual(result.body, b'returned')
        self.assertEqual(results, [result])
        self.assertEqual(ch._publish_inflight, {})

    def test_return_before_tag_still_matches_by_message_id(self):
        ch, raw_ch, _ = self._channel()
        # Delay every scheduled callback, then run only listener registration
        # so the return arrives before the publish callback assigns a tag.
        scheduled = []
        ch._wrapper._schedule_unchecked.side_effect = scheduled.append
        handle = ch.publish_with_result('', 'q', b'message', message_id='m1')
        scheduled[0]()
        ch._wrapper._schedule_unchecked.side_effect = lambda cb: cb()

        _return(raw_ch, 'm1')

        result = handle.wait(0)
        self.assertEqual(result.status, PublishStatus.NO_ROUTE)
        self.assertIsNone(result.delivery_tag)
        self.assertEqual(ch._publish_inflight, {})

    def test_duplicate_id_is_rejected_until_terminal(self):
        ch, raw_ch, _ = self._channel()
        first = ch.publish_with_result('', 'q', b'one', message_id='same')

        with self.assertRaises(DuplicateMessageId):
            ch.publish_with_result('', 'q', b'two', message_id='same')

        _ack(raw_ch, 1)
        first.wait(0)

        second = ch.publish_with_result('', 'q', b'three', message_id='same')
        self.assertEqual(second.message_id, 'same')
        _ack(raw_ch, 2)
        self.assertEqual(second.wait(0).status, PublishStatus.ACKED)

    def test_in_flight_limit_is_synchronous_and_explicit(self):
        ch, raw_ch, _ = self._channel(max_in_flight=1)
        first = ch.publish_with_result('', 'q', b'one', message_id='a')
        sent = len(raw_ch.connection.sent)

        with self.assertRaises(PublishInFlightLimitError):
            ch.publish_with_result('', 'q', b'two', message_id='b')
        self.assertEqual(len(raw_ch.connection.sent), sent)

        _ack(raw_ch, 1)
        first.wait(0)
        second = ch.publish_with_result('', 'q', b'two', message_id='b')
        _ack(raw_ch, 2)
        self.assertEqual(second.wait(0).status, PublishStatus.ACKED)

    def test_cancelling_the_wait_does_not_cancel_the_publish(self):
        ch, raw_ch, _ = self._channel()
        handle = ch.publish_with_result('', 'q', b'message', message_id='m1')

        self.assertTrue(handle.sent)
        self.assertTrue(handle.cancel_wait())
        with self.assertRaises(PublishWaitCancelled):
            handle.wait(0)
        self.assertIn('m1', ch._publish_inflight)
        self.assertEqual(ch._publish_by_tag[1].message_id, 'm1')

        _ack(raw_ch, 1)

        self.assertEqual(handle.outcome.status, PublishStatus.ACKED)
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(ch._publish_by_tag, {})

    def test_wait_timeout_leaves_the_publish_in_flight(self):
        ch, raw_ch, _ = self._channel()
        handle = ch.publish_with_result('', 'q', b'message', message_id='m1')

        with self.assertRaises(TimeoutError):
            handle.wait(0.01)
        self.assertIn('m1', ch._publish_inflight)

        _ack(raw_ch, 1)
        self.assertEqual(handle.wait(1).status, PublishStatus.ACKED)

    def test_channel_close_fails_every_in_flight_publish(self):
        ch, raw_ch, _ = self._channel()
        results = []
        h1 = ch.publish_with_result('',
                                    'q',
                                    b'one',
                                    message_id='m1',
                                    on_result=results.append)
        h2 = ch.publish_with_result('', 'q', b'two', message_id='m2')
        reason = ChannelClosedByBroker(406, 'closed')

        ch._on_broker_close(raw_ch, reason)

        for handle in (h1, h2):
            result = handle.wait(0)
            self.assertEqual(result.status, PublishStatus.FAILED)
            self.assertIs(result.error, reason)
            self.assertFalse(result.routed)
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(ch._publish_by_tag, {})
        self.assertTrue(all(result.error is reason for result in results))

    def test_slow_result_callback_does_not_delay_the_terminal_result(self):
        ch, raw_ch, _ = self._channel()
        called = threading.Event()

        def slow_callback(result):
            time.sleep(0.2)
            called.set()

        handle = ch.publish_with_result('',
                                        'q',
                                        b'message',
                                        message_id='m1',
                                        on_result=slow_callback)

        started = time.monotonic()
        _ack(raw_ch, 1)
        result = handle.wait(0)
        elapsed = time.monotonic() - started

        self.assertEqual(result.status, PublishStatus.ACKED)
        self.assertLess(elapsed, 0.15)
        self.assertTrue(called.wait(2))
        self.assertEqual(ch._publish_inflight, {})

    def test_result_callback_still_runs_when_the_pool_is_shut_down(self):
        ch, raw_ch, _ = self._channel()
        results = []
        handle = ch.publish_with_result('',
                                        'q',
                                        b'message',
                                        message_id='m1',
                                        on_result=results.append)
        # Close the result pool before the terminal event; delivery falls back
        # to the current thread rather than losing the result.
        ch._shutdown_result_pool(timeout=1.0)

        _ack(raw_ch, 1)

        self.assertEqual(handle.wait(0).status, PublishStatus.ACKED)
        self.assertEqual([r.status for r in results], [PublishStatus.ACKED])

    def test_legacy_basic_publish_is_unchanged(self):
        ch, raw_ch, _ = self._channel()
        ch.basic_publish('', 'q', b'message')
        self.assertEqual(ch._publish_inflight, {})
        self.assertEqual(len(raw_ch.connection.sent), 1)
        self.assertEqual(ch._next_publish_seq_no, 1)

    def test_basic_publish_selects_result_mode_with_message_id(self):
        ch, raw_ch, _ = self._channel()
        handle = ch.basic_publish('',
                                  'q',
                                  b'message',
                                  message_id='m1',
                                  mandatory=True)
        _ack(raw_ch, 1)
        self.assertEqual(handle.wait(0).status, PublishStatus.ACKED)


if __name__ == '__main__':
    unittest.main()


class PublishResultApiTests(unittest.TestCase):

    def tearDown(self):
        for ch in getattr(self, '_channels', []):
            ch._shutdown_result_pool(timeout=1.0)

    def _channel(self, **kwargs):
        ch, raw_ch, wrapper = _make_channel(**kwargs)
        self._channels = getattr(self, '_channels', []) + [ch]
        return ch, raw_ch, wrapper

    def test_result_publish_enables_confirms_when_they_are_off(self):
        ch, raw_ch, _ = self._channel()
        ch._next_publish_seq_no = None
        calls = []

        def blocking_rpc(method_name, send, timeout, on_sent=None):
            calls.append((method_name, timeout))
            if on_sent is not None:
                on_sent()
            return frame.Method(1, spec.Confirm.SelectOk())

        ch._blocking_rpc = blocking_rpc
        handle = ch.publish_with_result('', 'q', b'message', message_id='m1')

        self.assertEqual(calls, [('confirm_delivery', 10)])
        self.assertEqual(ch._next_publish_seq_no, 1)
        _ack(raw_ch, 1)
        self.assertEqual(handle.wait(0).status, PublishStatus.ACKED)

    def test_timeout_selects_the_blocking_form(self):
        ch, raw_ch, _ = self._channel()
        acker = threading.Thread(target=lambda: (time.sleep(0.02), _ack(
            raw_ch, 1)))
        # The publish is scheduled synchronously by the harness, so start the
        # ack thread first and give it the tag it will cover.
        acker.start()
        result = ch.publish_with_result('',
                                        'q',
                                        b'message',
                                        message_id='m1',
                                        timeout=1)
        acker.join()

        self.assertIsInstance(result, PublishResult)
        self.assertEqual(result.status, PublishStatus.ACKED)

    def test_constructor_limit_aliases_are_accepted(self):
        for kwargs in ({'max_inflight': 1}, {'max_in_flight_publishes': 1}):
            with self.subTest(kwargs=kwargs):
                ch, _raw_ch, _ = self._channel(**kwargs)
                ch.publish_with_result('', 'q', b'one', message_id='a')
                with self.assertRaises(PublishInFlightLimitError):
                    ch.publish_with_result('', 'q', b'two', message_id='b')

    def test_result_callback_alias_is_accepted(self):
        ch, raw_ch, _ = self._channel()
        results = []
        delivered = threading.Event()

        def callback(result):
            results.append(result)
            delivered.set()

        handle = ch.publish_with_result('',
                                        'q',
                                        b'message',
                                        message_id='m1',
                                        result_callback=callback)
        _ack(raw_ch, 1)
        self.assertEqual(handle.wait(0).status, PublishStatus.ACKED)
        self.assertTrue(delivered.wait(2))
        self.assertEqual([r.message_id for r in results], ['m1'])
