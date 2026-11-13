"""Wire-level regressions for concurrent publishing through `pika.Connection`.

The publisher threads never write to the socket. They marshal and schedule work, and a dedicated
thread runs the scheduled callbacks and the transport's writable callbacks. The scripted socket
below records every byte the transport actually accepts, so these tests assert on the AMQP frames
that went out rather than on how many times `basic_publish` was called.
"""

import collections
import errno
import threading
import unittest
from unittest import mock

import pika.callback
from pika import frame, spec
from pika.adapters import thread_safe_connection
from pika.adapters.utils import io_services_utils
from pika.connection import Connection as AsyncConnection
from pika.exceptions import ConnectionClosedByClient


class _FrameOrderError(AssertionError):
    """The byte stream crossed content frames that the protocol keeps serial."""


class _DecodedMessage:

    def __init__(self, channel_number, method):
        self.channel_number = channel_number
        self.exchange = method.exchange
        self.routing_key = method.routing_key
        self.mandatory = method.mandatory
        self.body_size = None
        self.properties = None
        self.body = b''

    def key(self):
        return (self.channel_number, self.exchange, self.routing_key,
                self.body_size, self.body)


class _WireParser:
    """Decode a byte stream into published messages, preserving per-channel order."""

    def __init__(self):
        self.messages = []
        self.active = {}
        self.tail = b''
        self.consumed = 0

    def feed(self, data):
        while self.consumed < len(data):
            consumed, decoded = frame.decode_frame(data, self.consumed)
            if consumed == 0:
                self.tail = data[self.consumed:]
                return
            self._accept(decoded)
            self.consumed += consumed

    def finish(self):
        if self.tail:
            raise _FrameOrderError(
                f'{len(self.tail)} trailing bytes do not form a frame')
        if self.active:
            raise _FrameOrderError(
                f'incomplete content on channels {sorted(self.active)}')

    def _accept(self, decoded):
        if isinstance(decoded, frame.Method):
            self._accept_method(decoded)
        elif isinstance(decoded, frame.Header):
            self._accept_header(decoded)
        elif isinstance(decoded, frame.Body):
            self._accept_body(decoded)
        else:
            raise _FrameOrderError(f'unexpected frame {decoded!r}')

    def _accept_method(self, decoded):
        if not isinstance(decoded.method, spec.Basic.Publish):
            raise _FrameOrderError(
                f'unexpected method {type(decoded.method).__name__}')
        channel = decoded.channel_number
        if channel in self.active:
            raise _FrameOrderError(
                f'channel {channel} started a message before completing the '
                f'previous one')
        self.active[channel] = _DecodedMessage(channel, decoded.method)

    def _accept_header(self, decoded):
        message = self.active.get(decoded.channel_number)
        if message is None:
            raise _FrameOrderError(
                f'header on channel {decoded.channel_number} without a method')
        if message.body_size is not None:
            raise _FrameOrderError(
                f'duplicate header on channel {decoded.channel_number}')
        message.body_size = decoded.body_size
        message.properties = decoded.properties
        if message.body_size == 0:
            self._complete(decoded.channel_number)

    def _accept_body(self, decoded):
        message = self.active.get(decoded.channel_number)
        if message is None or message.body_size is None:
            raise _FrameOrderError(
                f'body on channel {decoded.channel_number} without a header')
        message.body += decoded.fragment
        if len(message.body) > message.body_size:
            raise _FrameOrderError(
                f'channel {decoded.channel_number} body exceeds its header')
        if len(message.body) == message.body_size:
            self._complete(decoded.channel_number)

    def _complete(self, channel):
        message = self.active.pop(channel)
        self.messages.append(message)


class _ScriptedSocket:
    """A socket whose send() can short-write, block once, or fail at an offset."""

    def __init__(self, send_cap=None, would_block_at=(), fail_at=None):
        self.send_cap = send_cap
        self.would_block_at = set(would_block_at)
        self.fail_at = fail_at
        self.wire = bytearray()
        self.calls = []
        self.failures = []
        self._lock = threading.Lock()

    def fileno(self):
        return 1

    def send(self, data):
        with self._lock:
            self.calls.append(
                (threading.get_ident(), len(data), len(self.wire)))
            if self.fail_at is not None and len(self.wire) >= self.fail_at:
                error = ConnectionResetError(errno.ECONNRESET,
                                             'scripted send failure')
                self.failures.append(error)
                raise error
            ready = [offset for offset in self.would_block_at
                     if offset <= len(self.wire)]
            if ready:
                self.would_block_at.remove(min(ready))
                raise BlockingIOError(errno.EAGAIN, 'scripted would block')

            count = len(data)
            if self.send_cap is not None:
                count = min(count, self.send_cap)
            if self.fail_at is not None:
                count = min(count, self.fail_at - len(self.wire))
            if count <= 0:
                error = ConnectionResetError(errno.ECONNRESET,
                                             'scripted send failure')
                self.failures.append(error)
                raise error
            self.wire.extend(data[:count])
            return count

    def recv(self, _size):
        raise BlockingIOError(errno.EAGAIN, 'no reads in this harness')

    def shutdown(self, _how):
        pass

    def close(self):
        pass


class _FakeNbio:

    def __init__(self, loop):
        self._loop = loop
        self._lock = threading.Lock()
        self.reader = None
        self.writer = None
        self.writer_threads = set()

    def set_reader(self, _fd, callback):
        with self._lock:
            self.reader = callback

    def remove_reader(self, _fd):
        with self._lock:
            self.reader = None

    def set_writer(self, _fd, callback):
        with self._lock:
            self.writer = callback
            self.writer_threads.add(threading.get_ident())

    def remove_writer(self, _fd):
        with self._lock:
            self.writer = None

    def add_callback_threadsafe(self, callback):
        self._loop.add_callback_threadsafe(callback)

    def get_writer(self):
        with self._lock:
            return self.writer


class _Loop:
    """A minimal callback loop with pause/resume for deterministic enqueueing."""

    def __init__(self):
        self._condition = threading.Condition()
        self._callbacks = collections.deque()
        self._stopped = False
        self._paused = False
        self._parked = threading.Event()
        self._started = threading.Event()
        self.runner_ident = None

    def start(self):
        self.runner_ident = threading.get_ident()
        self._started.set()
        while True:
            with self._condition:
                while not self._stopped and (self._paused or
                                             not self._callbacks):
                    if self._paused:
                        self._parked.set()
                    self._condition.wait()
                self._parked.clear()
                if self._stopped:
                    return
                callback = self._callbacks.popleft()
            callback()

    def stop(self):
        with self._condition:
            self._stopped = True
            self._condition.notify_all()

    def add_callback_threadsafe(self, callback):
        if not callable(callback):
            raise TypeError('callback must be callable')
        with self._condition:
            self._callbacks.append(callback)
            self._condition.notify_all()

    add_callback = add_callback_threadsafe

    def wait_started(self, timeout=5):
        if not self._started.wait(timeout):
            raise AssertionError('IOLoop thread did not start')

    def pause(self, timeout=5):
        self._parked.clear()
        with self._condition:
            self._paused = True
            self._condition.notify_all()
        if not self._parked.wait(timeout):
            raise AssertionError('IOLoop thread did not park')

    def resume(self):
        with self._condition:
            self._paused = False
            self._condition.notify_all()

    def drain(self, timeout=5):
        done = threading.Event()

        def marker():
            done.set()

        self.add_callback_threadsafe(marker)
        if not done.wait(timeout):
            raise AssertionError('IOLoop thread did not reach the marker')

    def call(self, callback, timeout=5):
        done = threading.Event()
        errors = []

        def run():
            try:
                callback()
            except BaseException as exc:
                errors.append(exc)
            finally:
                done.set()

        self.add_callback_threadsafe(run)
        if not done.wait(timeout):
            raise AssertionError('IOLoop thread did not run the callback')
        if errors:
            raise errors[0]


class _Protocol:

    def __init__(self):
        self.lost = []

    def connection_lost(self, error):
        self.lost.append(error)

    def eof_received(self):
        return False

    def data_received(self, _data):
        pass


class _WireConnection:
    """The raw connection API a `pika.channel.Channel` needs, backed by the transport."""

    _body_max_length = spec.FRAME_MAX_SIZE - 8

    def __init__(self, transport, nbio):
        self._transport = transport
        self._nbio = nbio
        self.callbacks = pika.callback.CallbackManager()
        self.output_threads = set()
        self.residual_sizes = []
        self._error = None
        self.is_open = True
        self.is_closed = False

    def _marshal_message(self, channel_number, method, content):
        return AsyncConnection._marshal_message(self, channel_number, method,
                                                content)

    def _output_marshaled_frames(self, marshaled_frames):
        self.output_threads.add(threading.get_ident())
        for marshaled_frame in marshaled_frames:
            self._transport.write(marshaled_frame)
        self.pump()

    def pump(self):
        expected = threading.current_thread()
        if expected.ident != self._nbio._loop.runner_ident:
            raise AssertionError('transport pumped off the IOLoop thread')
        while self._transport._tx_buffers:
            if self._transport._state != self._transport._STATE_ACTIVE:
                return
            writer = self._nbio.get_writer()
            if writer is None:
                return
            writer()
            if self._transport._tx_buffers:
                self.residual_sizes.append(
                    self._transport.get_write_buffer_size())


class _FakeSelectConnection:

    def __init__(self, loop, wire_connection):
        self.ioloop = loop
        self._wire_connection = wire_connection
        self._on_open_callback = None
        self._on_close_callback = None
        self._closed = False
        self.is_open = True
        self.is_closing = False
        self.is_closed = False
        self.callbacks = self._wire_connection.callbacks

    def factory(self, **kwargs):
        self._on_open_callback = kwargs['on_open_callback']
        self._on_close_callback = kwargs['on_close_callback']
        self._on_open_callback(self)
        return self

    def channel(self, on_open_callback):
        raw_channel = pika.channel.Channel(self._wire_connection,
                                           self._wire_connection.channel_count,
                                           on_open_callback)
        self._wire_connection.channel_count += 1
        raw_channel._set_state(raw_channel.OPEN)
        on_open_callback(raw_channel)

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._wire_connection.pump()
        self.is_open = False
        self.is_closing = False
        self.is_closed = True
        self._on_close_callback(
            self, ConnectionClosedByClient(200, 'wire harness close'))


class _WireHarness:

    def __init__(self, send_cap=None, would_block_at=(), fail_at=None):
        self.loop = _Loop()
        self.socket = _ScriptedSocket(send_cap=send_cap,
                                      would_block_at=would_block_at,
                                      fail_at=fail_at)
        self.nbio = _FakeNbio(self.loop)
        self.protocol = _Protocol()
        self.transport = io_services_utils._AsyncPlaintextTransport(
            self.socket, self.protocol, self.nbio)
        self.wire_connection = _WireConnection(self.transport, self.nbio)
        self.wire_connection.channel_count = 1
        self.select_connection = _FakeSelectConnection(self.loop,
                                                       self.wire_connection)
        with mock.patch.object(thread_safe_connection,
                               'SelectConnection',
                               side_effect=self.select_connection.factory):
            self.connection = thread_safe_connection.Connection(
                parameters='params')
        self.ioloop = self.connection._ioloop_thread
        self.loop.wait_started()
        self.ioloop_ident = self.ioloop.ident

    def channel(self):
        return self.connection.channel(timeout=5)

    def close(self):
        try:
            if not self.connection.is_closed:
                self.connection.close(timeout=2)
        finally:
            self.loop.stop()
            self.ioloop.join(timeout=5)

    def parser(self):
        parser = _WireParser()
        parser.feed(bytes(self.socket.wire))
        return parser

    def drain(self):
        self.loop.drain()

    def pump(self):
        self.loop.call(self.wire_connection.pump)


def _marshal_message(channel_number, body):
    """Marshal one short publish without a connection or a socket."""
    connection = type('MarshalOnly', (), {})()
    connection._body_max_length = spec.FRAME_MAX_SIZE - 8
    method = spec.Basic.Publish(exchange='', routing_key='rk')
    content = (spec.BasicProperties(), body)
    return AsyncConnection._marshal_message(connection, channel_number, method,
                                            content)


class ConcurrentPublishWireTests(unittest.TestCase):

    def _publish_from_threads(self, channels, bodies, barrier=None):
        errors = []
        thread_ids = set()

        def publish(channel, body):
            try:
                thread_ids.add(threading.get_ident())
                barrier.wait(timeout=5)
                channel.basic_publish(exchange='',
                                      routing_key='rk',
                                      body=body)
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=publish, args=(channel, body),
                             name=f'publisher-{index}')
            for index, (channel, body) in enumerate(zip(channels, bodies))
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive(),
                             f'{thread.name} did not finish')
        if errors:
            raise errors[0]
        return thread_ids

    def test_same_channel_messages_survive_short_writes_and_would_block(self):
        """
        Every body is reconstructed from the bytes the socket accepted.

        The socket caps each send and raises EAGAIN at several offsets, so the transport keeps a
        suffix in `_tx_buffers` and resumes it later. Message order across publisher threads is not
        asserted; the decoder rejects a second method frame on a channel while that channel still
        has an unfinished content sequence.
        """
        harness = _WireHarness(send_cap=7,
                               would_block_at=(1, 2, 19, 71, 173))
        self.addCleanup(harness.close)
        channel = harness.channel()
        bodies = [f'body-{index:02d}'.encode() for index in range(8)]
        threads = self._publish_from_threads([channel] * len(bodies), bodies,
                                             threading.Barrier(len(bodies)))
        harness.drain()

        parser = harness.parser()
        parser.finish()

        self.assertEqual(
            collections.Counter(message.key() for message in parser.messages),
            collections.Counter((1, '', 'rk', len(body), body)
                                for body in bodies))
        self.assertTrue(all(message.body_size == len(message.body)
                            for message in parser.messages))
        self.assertEqual(harness.socket.failures, [])
        self.assertEqual(harness.socket.would_block_at, set())
        self.assertTrue(harness.wire_connection.residual_sizes)
        self.assertTrue(
            any(requested > harness.socket.send_cap
                for _tid, requested, _sent in harness.socket.calls))
        self.assertEqual(
            {thread_id for thread_id, _requested, _sent in harness.socket.calls},
            {harness.ioloop_ident})
        self.assertEqual(harness.wire_connection.output_threads,
                         {harness.ioloop_ident})
        self.assertEqual(harness.nbio.writer_threads, {harness.ioloop_ident})
        self.assertTrue(threads)
        self.assertNotIn(harness.ioloop_ident, threads)
        self.assertEqual(harness.transport.get_write_buffer_size(), 0)

    def test_cross_channel_messages_may_interleave_but_stay_intact(self):
        """Channels 1 and 2 each keep their own content sequence while sharing one wire."""
        harness = _WireHarness(send_cap=11, would_block_at=(3, 29, 101))
        self.addCleanup(harness.close)
        first = harness.channel()
        second = harness.channel()
        bodies = [f'msg-{index:02d}'.encode() for index in range(6)]
        channels = [first, second] * (len(bodies) // 2)
        self._publish_from_threads(channels, bodies,
                                   threading.Barrier(len(bodies)))
        harness.drain()

        parser = harness.parser()
        parser.finish()

        self.assertEqual(
            collections.Counter(message.key() for message in parser.messages),
            collections.Counter([
                (1, '', 'rk', len(bodies[0]), bodies[0]),
                (1, '', 'rk', len(bodies[2]), bodies[2]),
                (1, '', 'rk', len(bodies[4]), bodies[4]),
                (2, '', 'rk', len(bodies[1]), bodies[1]),
                (2, '', 'rk', len(bodies[3]), bodies[3]),
                (2, '', 'rk', len(bodies[5]), bodies[5]),
            ]))
        self.assertEqual(
            {thread_id for thread_id, _requested, _sent in harness.socket.calls},
            {harness.ioloop_ident})
        self.assertEqual(harness.socket.would_block_at, set())
        self.assertTrue(harness.wire_connection.residual_sizes)
        self.assertEqual(harness.transport.get_write_buffer_size(), 0)

    def test_close_after_a_scheduled_publish_still_writes_the_message(self):
        """
        A close queued behind a scheduled publish runs after it.

        The loop is paused while `basic_publish` returns, so its callback is already in the queue
        when the closing thread starts. Resuming the loop must run that callback before the close.
        """
        harness = _WireHarness()
        self.addCleanup(harness.close)
        channel = harness.channel()
        body = b'published-before-close'
        closed = threading.Event()

        harness.loop.pause()
        channel.basic_publish(exchange='', routing_key='rk', body=body)

        def close():
            harness.connection.close(timeout=2)
            closed.set()

        closer = threading.Thread(target=close, name='closer')
        closer.start()
        harness.loop.resume()

        self.assertTrue(closed.wait(5), 'close did not return')
        closer.join(timeout=5)
        self.assertFalse(closer.is_alive())

        parser = harness.parser()
        parser.finish()
        self.assertEqual([message.key() for message in parser.messages],
                         [(1, '', 'rk', len(body), body)])

    def test_fatal_send_failure_leaves_only_a_complete_prefix(self):
        """
        A send error aborts the transport and discards the buffered suffix.

        All messages have the same wire size, so the failure offset cuts the same frame whether the
        publisher threads ran in this order or another. Two complete messages must remain; the third
        is a partial body in the captured bytes and nothing is replayed after the abort.
        """
        message_frames = _marshal_message(1, b'body-00')
        message_size = sum(len(part) for part in message_frames)
        fail_at = (2 * message_size + len(message_frames[0]) +
                   len(message_frames[1]) + 3)
        harness = _WireHarness(send_cap=7, fail_at=fail_at)
        self.addCleanup(harness.close)
        channel = harness.channel()
        bodies = [b'body-00', b'body-01', b'body-02']
        self._publish_from_threads([channel] * len(bodies), bodies,
                                   threading.Barrier(len(bodies)))
        harness.drain()

        parser = harness.parser()
        self.assertEqual(len(parser.messages), 2)
        self.assertEqual(len({message.body for message in parser.messages}), 2)
        self.assertEqual(len(parser.active), 1)
        partial = next(iter(parser.active.values()))
        self.assertEqual(partial.body_size, len(bodies[0]))
        self.assertLess(len(partial.body), partial.body_size)
        self.assertEqual(len(harness.socket.wire), fail_at)
        self.assertTrue(harness.socket.failures)
        self.assertEqual(harness.transport.get_write_buffer_size(), 0)
        self.assertEqual(list(harness.transport._tx_buffers), [])
        self.assertNotEqual(harness.transport._state,
                            harness.transport._STATE_ACTIVE)

        before = bytes(harness.socket.wire)
        harness.pump()
        self.assertEqual(bytes(harness.socket.wire), before)

    def test_publish_callback_exception_does_not_drop_later_messages(self):
        """An `on_publish` failure is logged and the wire still carries every message."""
        harness = _WireHarness(send_cap=9)
        self.addCleanup(harness.close)
        channel = harness.channel()
        channel._next_publish_seq_no = 0
        bodies = [f'callback-{index}'.encode() for index in range(4)]
        tags = []

        def fail(tag):
            tags.append(tag)
            raise RuntimeError('callback failure')

        with self.assertLogs('pika.adapters.thread_safe_connection',
                             level='ERROR'):
            threads = []
            barrier = threading.Barrier(len(bodies))
            for body in bodies:

                def publish(body=body):
                    barrier.wait(timeout=5)
                    channel.basic_publish(exchange='',
                                          routing_key='rk',
                                          body=body,
                                          on_publish=fail)

                thread = threading.Thread(target=publish)
                thread.start()
                threads.append(thread)
            for thread in threads:
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())

        harness.drain()
        parser = harness.parser()
        parser.finish()
        self.assertEqual(
            collections.Counter(message.key() for message in parser.messages),
            collections.Counter((1, '', 'rk', len(body), body)
                                for body in bodies))
        self.assertEqual(sorted(tags), [1, 2, 3, 4])
        self.assertTrue(harness.connection.is_open)


class FrameDetectorTests(unittest.TestCase):
    """Negative controls for the decoder used by the wire tests."""

    def _crossed_bytes(self, channel_numbers):
        first = _marshal_message(channel_numbers[0], b'first')
        second = _marshal_message(channel_numbers[1], b'second')
        # Method and header of each message overtake the other channel's body.
        return b''.join(
            (first[0], first[1], second[0], second[1], first[2], second[2]))

    def test_crossed_content_frames_on_one_channel_are_rejected(self):
        parser = _WireParser()
        with self.assertRaisesRegex(_FrameOrderError,
                                    'started a message before completing'):
            parser.feed(self._crossed_bytes((1, 1)))

    def test_crossed_content_frames_across_channels_are_accepted(self):
        parser = _WireParser()
        parser.feed(self._crossed_bytes((1, 2)))
        parser.finish()
        self.assertEqual(
            collections.Counter(message.key() for message in parser.messages),
            collections.Counter([
                (1, '', 'rk', 5, b'first'),
                (2, '', 'rk', 6, b'second'),
            ]))

    def test_duplicate_body_frame_is_rejected(self):
        first = _marshal_message(1, b'body')
        parser = _WireParser()
        with self.assertRaisesRegex(_FrameOrderError, 'without a header'):
            parser.feed(b''.join((first[0], first[1], first[2], first[2])))

    def test_body_without_a_header_is_rejected(self):
        first = _marshal_message(1, b'body')
        parser = _WireParser()
        with self.assertRaisesRegex(_FrameOrderError, 'without a header'):
            parser.feed(b''.join((first[0], first[2])))


if __name__ == '__main__':
    unittest.main()
