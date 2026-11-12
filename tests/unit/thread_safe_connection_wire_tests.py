"""Wire-level regression tests for concurrent publishing on ThreadSafeConnection.

The existing thread_safe_connection tests assert at the mock-call layer; these
drive a real SelectConnection over a controllable socket double and re-parse
the bytes that actually leave the process.  A fake broker performs the AMQP
handshake so connection and channel objects are the production classes,
including the real _AsyncPlaintextTransport, its _tx_buffers and its short-write
retry loop.
"""

import errno
import socket
import threading
import time
import unittest
from unittest.mock import patch

import pika
import pika.adapters.utils.connection_workflow as connection_workflow
import pika.adapters.utils.io_services_utils as io_services_utils
import pika.adapters.utils.selector_ioloop_adapter as selector_ioloop_adapter
from pika import exceptions, frame, spec
from pika.adapters.thread_safe_connection import Connection

#: Seconds every bounded wait in this module gets; the pytest timeout is the
#: backstop, these keep a failure local.
WAIT = 10


class _SocketModuleShim:
    """
    Replaces the ``socket`` name in one module's namespace without touching the
    real socket module, so other threads keep the standard constructors.

    Unknown attributes delegate to the real module.
    """

    def __init__(self, **overrides):
        self._overrides = overrides

    def __getattr__(self, name):
        try:
            return self._overrides[name]
        except KeyError:
            return getattr(socket, name)


class ControllableSocketDouble:
    """
    Socket double sitting in front of an AF_UNIX socketpair.

    Every send records its caller thread and its bytes, and each send can:
    short-write (cap bytes accepted), raise EAGAIN (temporarily not writable),
    or fail with a fatal OSError after a fixed number of accepted bytes.  The
    transport underneath keeps its real _tx_buffers retry semantics.
    """

    def __init__(self, real_sock):
        self._real = real_sock
        self.family = socket.AF_INET
        self.type = socket.SOCK_STREAM
        self.proto = 0
        self._lock = threading.Lock()
        self.send_calls = []
        self.recv_calls = []
        self.accepted_bytes = 0
        self.would_block_count = 0
        #: Cap bytes accepted per send; None means the whole chunk.
        self.max_send = None
        #: monotonic deadline before which send raises EAGAIN.
        self.block_until = 0.0
        #: Fail with ECONNRESET once this many bytes have been accepted.
        self.fail_after = None
        self.fatal_error = None

    def fileno(self):
        return self._real.fileno()

    def setblocking(self, flag):
        return self._real.setblocking(flag)

    def settimeout(self, value):
        return self._real.settimeout(value)

    def setsockopt(self, level, option, value=None):
        # TCP-only options (TCP_NODELAY) do not apply to the AF_UNIX pair.
        return None

    def getsockopt(self, level, option, *args):
        if level == socket.SOL_SOCKET and option == socket.SO_ERROR:
            return 0
        return self._real.getsockopt(level, option, *args)

    def connect(self, address):
        # The pair is already connected; mimic a non-blocking connect in
        # progress and let the writable watcher complete the workflow.
        raise BlockingIOError(errno.EINPROGRESS, 'simulated connect in progress')

    def send(self, data):
        name = threading.current_thread().name
        with self._lock:
            self.send_calls.append((name, bytes(data)))
            if time.monotonic() < self.block_until:
                self.would_block_count += 1
                raise BlockingIOError(errno.EAGAIN, 'simulated not writable')
            n = len(data)
            if self.max_send is not None:
                n = min(n, self.max_send)
            if self.fail_after is not None:
                remaining = self.fail_after - self.accepted_bytes
                if remaining <= 0:
                    self.fatal_error = OSError(errno.ECONNRESET,
                                               'simulated send failure')
                    raise self.fatal_error
                n = min(n, remaining)
            sent = self._real.send(data[:n])
            self.accepted_bytes += sent
            return sent

    def recv(self, size):
        with self._lock:
            self.recv_calls.append(threading.current_thread().name)
        return self._real.recv(size)

    def shutdown(self, how):
        try:
            return self._real.shutdown(how)
        except OSError:
            return None

    def close(self):
        return self._real.close()

    def getpeername(self):
        return ('127.0.0.1', 5672)

    def getsockname(self):
        return ('127.0.0.1', 12345)

    def __repr__(self):
        return f'<ControllableSocketDouble fd={self.fileno()}>'


class FakeBroker(threading.Thread):
    """
    Minimal AMQP peer: completes the connection handshake, opens channels,
    answers Confirm.Select, and echoes Close frames, then records every byte
    the client sends for later re-parsing.
    """

    def __init__(self, peer_sock, tune_frame_max=spec.FRAME_MAX_SIZE):
        super().__init__(name='fake-broker', daemon=True)
        self._sock = peer_sock
        self._tune_frame_max = tune_frame_max
        self._buf = b''
        self.stop_event = threading.Event()
        self.observed = bytearray()
        self.error = None

    def _read_frame(self):
        while True:
            consumed, frm = frame.decode_frame(self._buf)
            if frm is not None:
                self._buf = self._buf[consumed:]
                return frm
            if self.stop_event.is_set():
                return None
            data = self._sock.recv(65536)
            if not data:
                return None
            self.observed.extend(data)
            self._buf += data

    def _send(self, frm):
        self._sock.sendall(frm.marshal())

    def run(self):
        try:
            self._run()
        except Exception as exc:  # surfaced by the harness, not the thread
            self.error = exc

    def _run(self):
        proto = self._read_frame()
        assert isinstance(proto, frame.ProtocolHeader), proto
        self._send(
            frame.Method(
                0,
                spec.Connection.Start(
                    version_major=0,
                    version_minor=9,
                    server_properties={
                        'product': 'fake-broker',
                        'capabilities': {
                            'publisher_confirms': True,
                            'basic.nack': True,
                        },
                    },
                    mechanisms='PLAIN',
                    locales='en_US')))
        start_ok = self._read_frame()
        assert isinstance(start_ok, frame.Method), start_ok
        self._send(frame.Method(0, spec.Connection.Tune(0, self._tune_frame_max, 0)))
        tune_ok = self._read_frame()
        assert isinstance(tune_ok, frame.Method), tune_ok
        open_frame = self._read_frame()
        assert isinstance(open_frame.method, spec.Connection.Open), open_frame
        self._send(frame.Method(0, spec.Connection.OpenOk()))
        while not self.stop_event.is_set():
            frm = self._read_frame()
            if frm is None:
                return
            if not isinstance(frm, frame.Method):
                continue
            method = frm.method
            if isinstance(method, spec.Channel.Open):
                self._send(frame.Method(frm.channel_number, spec.Channel.OpenOk()))
            elif isinstance(method, spec.Confirm.Select):
                self._send(frame.Method(frm.channel_number, spec.Confirm.SelectOk()))
            elif isinstance(method, spec.Channel.Close):
                self._send(frame.Method(frm.channel_number, spec.Channel.CloseOk()))
            elif isinstance(method, spec.Connection.Close):
                self._send(frame.Method(0, spec.Connection.CloseOk()))
                return


class PublishMessage:
    """One re-parsed Basic.Publish: method, header and concatenated body."""

    def __init__(self, method, header, body):
        self.method = method
        self.header = header
        self.body = body


def frame_offsets(raw):
    """Yield the start offset of every complete frame in *raw*."""
    pos = 0
    while pos < len(raw):
        consumed, _frm = frame.decode_frame(raw, pos)
        if _frm is None:
            raise AssertionError(
                f'undecodable trailing bytes at offset {pos} of {len(raw)}')
        yield pos
        pos += consumed


def parse_publishes(raw, strict=True):
    """
    Re-parse raw output bytes into per-channel publish sequences.

    Enforces the content-frame protocol a real broker would: a Basic.Publish
    method frame starts a message, exactly one header follows it, and body
    frames complete it; anything else on that channel in between is a protocol
    violation and raises AssertionError.  Frames on other channels may
    interleave freely, which is the only reordering AMQP permits.

    :param strict: When False, a message whose tail never arrived (a transport
        that died mid-send) does not raise; its complete messages are still
        returned.  Structural violations are rejected in either mode.
    :returns: dict mapping channel number to a list of PublishMessage, in wire
        order for that channel.
    """
    per_channel = {}
    pending = {}
    pos = 0
    while pos < len(raw):
        consumed, frm = frame.decode_frame(raw, pos)
        if frm is None:
            raise AssertionError(
                f'undecodable trailing bytes at offset {pos} of {len(raw)}')
        pos += consumed
        if isinstance(frm, frame.ProtocolHeader):
            continue
        channel = frm.channel_number
        if isinstance(frm, frame.Method):
            if isinstance(frm.method, spec.Basic.Publish):
                if channel in pending:
                    raise AssertionError(
                        f'channel {channel}: Basic.Publish arrived while the '
                        f'previous message was incomplete')
                pending[channel] = {'method': frm.method, 'header': None, 'body': []}
            continue
        if isinstance(frm, frame.Header):
            message = pending.get(channel)
            if message is None or message['header'] is not None:
                raise AssertionError(
                    f'channel {channel}: content header without a pending '
                    f'Basic.Publish')
            message['header'] = frm
            continue
        if isinstance(frm, frame.Body):
            message = pending.get(channel)
            if message is None or message['header'] is None:
                raise AssertionError(
                    f'channel {channel}: content body before its header')
            message['body'].append(frm.fragment)
            body = b''.join(message['body'])
            if len(body) > message['header'].body_size:
                raise AssertionError(
                    f'channel {channel}: body exceeds the declared '
                    f'{message["header"].body_size} bytes')
            if len(body) == message['header'].body_size:
                per_channel.setdefault(channel, []).append(
                    PublishMessage(message['method'], message['header'], body))
                del pending[channel]
            continue
    if strict and pending:
        raise AssertionError(
            f'incomplete messages on channels {sorted(pending)}')
    return per_channel


class _WireHarness(unittest.TestCase):
    """
    Builds a `Connection` whose SelectConnection runs its real IOLoop thread
    against a `ControllableSocketDouble`, with a `FakeBroker` on the other end
    of the pair.
    """

    def _make_connection(self,
                         tune_frame_max=spec.FRAME_MAX_SIZE,
                         extra_patches=()):
        client_real, broker_sock = socket.socketpair()
        double = ControllableSocketDouble(client_real)
        broker = FakeBroker(broker_sock, tune_frame_max=tune_frame_max)
        broker.start()

        addr = (socket.AF_INET, socket.SOCK_STREAM, 0, '', ('127.0.0.1', 5672))
        real_socket = socket.socket

        def socket_factory(*args, **kwargs):
            # socket.socketpair() builds its ends through this same
            # constructor with a fileno; only the workflow's own socket is
            # replaced, everything else gets a real socket.
            if 'fileno' in kwargs or len(args) > 3:
                return real_socket(*args, **kwargs)
            return double

        def getaddrinfo(*args, **kwargs):
            return [addr]

        patches = [
            patch.object(connection_workflow,
                         'socket',
                         _SocketModuleShim(socket=socket_factory)),
            patch.object(selector_ioloop_adapter,
                         'socket',
                         _SocketModuleShim(getaddrinfo=getaddrinfo)),
            *extra_patches,
        ]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

        conn = Connection(pika.ConnectionParameters('127.0.0.1', heartbeat=0))

        def _teardown():
            # Close first while the broker still answers frames; stopping it
            # first would leave Connection.Close without a CloseOk and make
            # the close budget expire on every test.
            try:
                conn.close(timeout=WAIT)
            except Exception:
                pass
            broker.stop_event.set()
            broker.join(timeout=WAIT)
            self.assertFalse(
                broker.is_alive(), 'fake broker thread outlived its test')
            if broker.error is not None:
                raise AssertionError(
                    f'fake broker failed: {broker.error!r}') from broker.error
            client_real.close()
            broker_sock.close()
            ioloop = getattr(conn._connection, 'ioloop', None)
            if ioloop is not None:
                try:
                    ioloop.close()
                except Exception:
                    pass

        self.addCleanup(_teardown)
        return conn, double, broker

    def _drain_tx_buffers(self, conn):
        """
        Wait until every scheduled publish has been marshaled and flushed.

        A marker callback runs after every already-scheduled ``_publish`` (the
        IOLoop callback queue is FIFO), so once it fires the frames are all in
        ``_tx_buffers``; only then does waiting for the buffer to empty mean
        the double accepted every frame.
        """
        flushed = threading.Event()
        conn.add_callback_threadsafe(flushed.set)
        self.assertTrue(flushed.wait(timeout=WAIT),
                        'IOLoop did not run the queued callbacks')
        transport = conn._connection._transport
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            if transport is None or not transport._tx_buffers:
                return
            time.sleep(0.01)
        self.fail('send buffer did not drain; '
                  f'{transport._tx_buffered_byte_count} bytes left')

    def _wait_until_broker_observed(self, broker, double):
        """
        Wait until the broker has read every byte the double accepted.

        An empty client `_tx_buffers` only means the last send returned; those
        bytes can still be in flight across the pair, so assertions against the
        broker's recorded stream must sync on this, not the client buffer.
        """
        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline:
            if len(broker.observed) >= double.accepted_bytes:
                return
            time.sleep(0.01)
        self.fail(
            f'broker observed {len(broker.observed)} of '
            f'{double.accepted_bytes} accepted bytes')

    def _publish_from_threads(self, channel, bodies):
        """
        Publish every body from its own thread, all released by one barrier so
        the interleaving is up to the scheduler, not the test.
        """
        barrier = threading.Barrier(len(bodies))
        errors = []
        errors_lock = threading.Lock()

        def publish(body):
            barrier.wait(timeout=WAIT)
            try:
                channel.basic_publish(exchange='',
                                      routing_key='q',
                                      body=body)
            except Exception as exc:
                with errors_lock:
                    errors.append(exc)

        threads = [
            threading.Thread(target=publish, args=(body,), name=f'pub-{i}')
            for i, body in enumerate(bodies)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=WAIT)
        for thread in threads:
            self.assertFalse(thread.is_alive(),
                             f'{thread.name} did not finish publishing')
        if errors:
            raise errors[0]

    def _assert_io_confined_to_ioloop(self, conn, double):
        ioloop_name = conn._ioloop_thread.name
        offenders = sorted({
            name
            for name, _data in double.send_calls if name != ioloop_name
        } | {name for name in double.recv_calls if name != ioloop_name})
        self.assertEqual(
            offenders, [],
            f'socket I/O escaped the IOLoop thread {ioloop_name!r}')


class ConcurrentPublishWireTests(_WireHarness):
    """Concurrent publishers, re-parsed from the bytes on the wire."""

    def test_same_channel_concurrent_publish_under_short_writes(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        double.max_send = 3
        double.block_until = time.monotonic() + 0.3

        bodies = [f'msg-{i}'.encode() for i in range(8)]
        self._publish_from_threads(channel, bodies)
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self.assertEqual(double.fatal_error, None)
        self._assert_io_confined_to_ioloop(conn, double)
        messages = parse_publishes(bytes(broker.observed))
        got = sorted(message.body for message in messages[channel.channel_number])
        self.assertEqual(got, sorted(bodies))
        for message in messages[channel.channel_number]:
            self.assertEqual(message.header.body_size, len(message.body))
            self.assertEqual(message.method.routing_key, 'q')

    def test_body_spanning_multiple_content_frames(self):
        conn, double, broker = self._make_connection(tune_frame_max=8192)
        channel = conn.channel()
        double.max_send = 64
        body_length = 9000

        bodies = [bytes([i]) * body_length for i in range(4)]
        self._publish_from_threads(channel, bodies)
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self._assert_io_confined_to_ioloop(conn, double)
        wire = bytes(broker.observed)
        body_frame_count = sum(
            frame.decode_frame(wire, pos)[1].frame_type == spec.FRAME_BODY
            for pos in frame_offsets(wire))
        self.assertGreater(body_frame_count, len(bodies))
        messages = parse_publishes(wire)
        got = sorted(message.body for message in messages[channel.channel_number])
        self.assertEqual(got, sorted(bodies))
        for message in messages[channel.channel_number]:
            self.assertEqual(message.header.body_size, body_length)

    def test_cross_channel_interleaving_keeps_per_channel_order(self):
        conn, double, broker = self._make_connection()
        channel_a = conn.channel()
        channel_b = conn.channel()
        double.max_send = 5

        bodies_a = [f'a-{i}'.encode() for i in range(4)]
        bodies_b = [f'b-{i}'.encode() for i in range(4)]
        barrier = threading.Barrier(2)

        def publish_all(channel, bodies):
            barrier.wait(timeout=WAIT)
            for body in bodies:
                channel.basic_publish(exchange='', routing_key='q', body=body)

        threads = [
            threading.Thread(target=publish_all, args=(channel_a, bodies_a)),
            threading.Thread(target=publish_all, args=(channel_b, bodies_b)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=WAIT)
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self._assert_io_confined_to_ioloop(conn, double)
        messages = parse_publishes(bytes(broker.observed))
        # Each channel's frames stay in publish order on the wire; the two
        # channels may interleave arbitrarily between them.
        self.assertEqual(
            [m.body for m in messages[channel_a.channel_number]], bodies_a)
        self.assertEqual(
            [m.body for m in messages[channel_b.channel_number]], bodies_b)


class TransportFaultWireTests(_WireHarness):
    """Short writes, writability stalls and fatal failures, by byte evidence."""

    def test_temporarily_unwritable_retries_without_loss(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        double.block_until = time.monotonic() + 0.5
        double.max_send = 11

        bodies = [f'blocked-{i}'.encode() for i in range(6)]
        self._publish_from_threads(channel, bodies)
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self.assertGreater(double.would_block_count, 0)
        self._assert_io_confined_to_ioloop(conn, double)
        messages = parse_publishes(bytes(broker.observed))
        got = sorted(m.body for m in messages[channel.channel_number])
        self.assertEqual(got, sorted(bodies))

    def test_fatal_failure_strands_unflushed_buffer_and_closes_connection(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        transport = conn._connection._transport

        snapshot = {}
        original_initiate_abort = transport._initiate_abort

        def snapshot_then_abort(error):
            # Observed before _deactivate() clears the deque, so the bytes
            # the failure stranded are visible rather than post-hoc guessed.
            snapshot['byte_count'] = transport._tx_buffered_byte_count
            snapshot['thread'] = threading.current_thread().name
            return original_initiate_abort(error)

        transport._initiate_abort = snapshot_then_abort

        handshake_bytes = double.accepted_bytes
        double.fail_after = handshake_bytes + 40
        double.max_send = 7

        self._publish_from_threads(
            channel, [f'fail-{i}'.encode() for i in range(8)])

        deadline = time.monotonic() + WAIT
        while time.monotonic() < deadline and conn._closed_reason is None:
            time.sleep(0.01)
        self.assertIsNotNone(conn._closed_reason)
        self.assertIsInstance(conn._closed_reason,
                              exceptions.StreamLostError)
        self.assertIsNotNone(double.fatal_error)

        # The failure hit with data still queued, not on an empty buffer.
        self.assertGreater(snapshot['byte_count'], 0)
        self.assertEqual(snapshot['thread'], conn._ioloop_thread.name)

        # What reached the broker is a prefix of the accepted bytes; messages
        # not fully flushed are gone, never silently resent.
        wire = bytes(broker.observed)
        self.assertLessEqual(len(wire), double.accepted_bytes)
        partial = parse_publishes(wire, strict=False)
        flushed = [
            m.body for messages in partial.values() for m in messages
        ]
        self.assertLess(len(flushed), 8)
        self.assertEqual(len(flushed), len(set(flushed)))

        with self.assertRaises(exceptions.StreamLostError):
            channel.basic_publish(exchange='', routing_key='q', body=b'after')
        self._assert_io_confined_to_ioloop(conn, double)

    def test_every_send_happens_on_the_ioloop_thread(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        double.max_send = 2
        double.block_until = time.monotonic() + 0.2
        self._publish_from_threads(
            channel, [f'thread-check-{i}'.encode() for i in range(6)])
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self.assertTrue(double.send_calls)
        self._assert_io_confined_to_ioloop(conn, double)
        self.assertTrue(
            all(name == conn._ioloop_thread.name
                for name, _data in double.send_calls))
        self.assertTrue(
            all(name == conn._ioloop_thread.name
                for name in double.recv_calls))


class CloseAndCallbackWireTests(_WireHarness):
    """Publishes already enqueued, and user callbacks that raise."""

    def test_publishes_enqueued_before_close_reach_the_wire(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        double.max_send = 4
        bodies = [f'close-{i}'.encode() for i in range(5)]
        for body in bodies:
            channel.basic_publish(exchange='', routing_key='q', body=body)

        conn.close(timeout=WAIT)

        messages = parse_publishes(bytes(broker.observed))
        got = sorted(m.body for m in messages[channel.channel_number])
        self.assertEqual(got, sorted(bodies))

    def test_raising_on_publish_is_contained_and_frame_still_sent(self):
        conn, double, broker = self._make_connection()
        channel = conn.channel()
        channel.confirm_delivery(lambda method_frame: None)
        seen_tags = []
        seen_lock = threading.Lock()

        def raising_callback(delivery_tag):
            with seen_lock:
                seen_tags.append(delivery_tag)
            raise RuntimeError('user on_publish failure')

        bodies = [f'cb-{i}'.encode() for i in range(4)]
        barrier = threading.Barrier(len(bodies))
        errors = []

        def publish_with_callback(body):
            barrier.wait(timeout=WAIT)
            try:
                channel.basic_publish(exchange='',
                                      routing_key='q',
                                      body=body,
                                      on_publish=raising_callback)
            except Exception as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=publish_with_callback, args=(body,))
            for body in bodies
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=WAIT)
        self.assertEqual(errors, [])
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, double)

        self.assertTrue(conn.is_open)
        self.assertEqual(sorted(seen_tags), [1, 2, 3, 4])
        messages = parse_publishes(bytes(broker.observed))
        wire_bodies = sorted(
            m.body for m in messages[channel.channel_number])
        self.assertEqual(wire_bodies, sorted(bodies))


class _CrossingTransport(io_services_utils._AsyncPlaintextTransport):
    """
    A deliberately broken transport: each content body is queued ahead of its
    own header, crossing the frames of a single message on the wire.
    """

    def _buffer_tx_data(self, data):
        if len(data) > 7:
            frame_type = data[0]
            if frame_type == spec.FRAME_HEADER:
                self._held_header = data
                return
            if frame_type == spec.FRAME_BODY and getattr(
                    self, '_held_header', None) is not None:
                super()._buffer_tx_data(data)
                super()._buffer_tx_data(self._held_header)
                self._held_header = None
                return
        super()._buffer_tx_data(data)


class DetectorEffectivenessTests(_WireHarness):
    """The re-parser must catch the corruption it claims to detect."""

    def test_crossed_content_frames_are_detected(self):
        conn, _double, broker = self._make_connection(extra_patches=(
            patch.object(io_services_utils, '_AsyncPlaintextTransport',
                         _CrossingTransport), ))
        channel = conn.channel()
        channel.basic_publish(exchange='', routing_key='q', body=b'crossed')
        self._drain_tx_buffers(conn)
        self._wait_until_broker_observed(broker, _double)
        conn.close(timeout=WAIT)

        with self.assertRaises(AssertionError) as caught:
            parse_publishes(bytes(broker.observed))
        self.assertIn('body before its header', str(caught.exception))

    def test_parser_rejects_a_body_larger_than_declared(self):
        method = frame.Method(1, spec.Basic.Publish(routing_key='q')).marshal()
        header = frame.Header(1, 3, spec.BasicProperties()).marshal()
        body = frame.Body(1, b'toolong').marshal()
        with self.assertRaises(AssertionError):
            parse_publishes(method + header + body)

    def test_parser_rejects_an_incomplete_message(self):
        method = frame.Method(1, spec.Basic.Publish(routing_key='q')).marshal()
        header = frame.Header(1, 3, spec.BasicProperties()).marshal()
        with self.assertRaises(AssertionError):
            parse_publishes(method + header)
