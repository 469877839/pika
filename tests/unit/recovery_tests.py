"""Tests for pika.recovery, driven by frames from a fake broker."""

import unittest
from unittest import mock

from pika import callback, channel, exceptions, frame, recovery, spec


class _FakeConnection:
    """A connection that records what each channel sends and holds real callbacks."""

    def __init__(self) -> None:
        self.callbacks = callback.CallbackManager()
        self.accept_consumer_cancel_ok = False
        self.sent = []
        self.published = []

    def _send_method(self, channel_number, method, content=None):
        self.sent.append((channel_number, method))

    def _marshal_message(self, channel_number, method, content):
        return [method]

    def _output_marshaled_frames(self, frames):
        self.published.extend(frames)


def _open_channel(connection, channel_number):
    """Create and open a channel, driving the broker's OpenOk."""
    opened = []
    chan = channel.Channel(connection, channel_number, opened.append)
    chan.open()
    _deliver(connection, chan, spec.Channel.OpenOk())
    assert opened == [chan]
    return chan


def _deliver(connection, chan, method):
    """Deliver a broker method frame to the channel's real callback stack."""
    reply = frame.Method(chan.channel_number, method)
    connection.callbacks.process(chan.channel_number, method, connection, reply)
    return reply


def _confirm(frame_value):
    """A completion callback that ignores the reply."""
    return lambda _frame: None


class RecoveryPlanTests(unittest.TestCase):

    def setUp(self):
        self.connection = _FakeConnection()
        self.plan = recovery.ChannelRecoveryPlan()
        self.old = _open_channel(self.connection, 1)
        self.old.set_recovery_plan(self.plan)
        self.results = []

    def _declared(self, queue='orders', actual_name='orders'):
        self.old.queue_declare(queue, durable=True, callback=_confirm(None))
        return _deliver(self.connection, self.old,
                        spec.Queue.DeclareOk(queue=actual_name))

    def _sent_on(self, chan):
        return [
            method for number, method in self.connection.sent
            if number == chan.channel_number
        ]

    def _apply(self):
        self.new = _open_channel(self.connection, 2)
        self.plan.apply(self.new, self.results.append)
        return self.new

    def _record_full_topology(self):
        """Establish the five covered operations on the old channel, in order."""
        self.old.exchange_declare('events',
                                  'topic',
                                  durable=True,
                                  callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Exchange.DeclareOk())
        self._declared()
        self.old.queue_bind('orders',
                            'events',
                            'order.created',
                            callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.BindOk())
        self.old.basic_qos(prefetch_count=5, callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.QosOk())
        self.old.basic_consume('orders',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))

    def test_records_only_broker_confirmed_topology(self):
        self._record_full_topology()

        recorded = self.plan.operations
        self.assertEqual(len(recorded), 5)
        self.assertIsInstance(recorded[0], recovery.ExchangeDeclareRecord)
        self.assertEqual(recorded[0].exchange_type, 'topic')
        self.assertIsInstance(recorded[1], recovery.QueueDeclareRecord)
        self.assertIsInstance(recorded[2], recovery.QueueBindRecord)
        self.assertEqual(recorded[2].routing_key, 'order.created')
        self.assertIsInstance(recorded[3], recovery.QosRecord)
        self.assertEqual(recorded[3].prefetch_count, 5)
        self.assertIsInstance(recorded[4], recovery.ConsumeRecord)
        self.assertEqual(recorded[4].consumer_tag, 'ctag')

        # A publish and a nowait declaration are not broker-confirmed topology.
        self.old.basic_publish('events', 'order.created', b'body')
        self.old.queue_declare('nowait', callback=None)
        self.assertEqual(len(self.plan), 5)

    def test_queue_delete_and_cancel_remove_confirmed_records(self):
        self._declared()
        self.old.queue_bind('orders', 'events', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.BindOk())
        self.old.basic_consume('orders',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))
        self.assertEqual(len(self.plan), 3)

        self.old.queue_delete('orders', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.DeleteOk())
        self.assertEqual(self.plan.operations, ())

        self.old.basic_consume('other',
                               mock.Mock(),
                               consumer_tag='ctag2',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag2'))
        self.assertEqual(len(self.plan), 1)
        self.old.basic_cancel('ctag2', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.CancelOk('ctag2'))
        self.assertEqual(self.plan.operations, ())

    def test_a_nowait_cancel_keeps_the_consumer_record(self):
        self.old.basic_consume('orders',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))

        # No CancelOk is expected, so the broker never confirms the cancel.
        self.old.basic_cancel('ctag')

        self.assertEqual(len(self.plan), 1)

    def test_broker_cancel_removes_consumer_record(self):
        self.old.basic_consume('orders',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))
        self.assertEqual(len(self.plan), 1)

        _deliver(self.connection, self.old, spec.Basic.Cancel('ctag'))
        self.assertEqual(self.plan.operations, ())

    def test_deleting_a_generated_queue_by_empty_name_drops_its_records(self):
        self._declared('', actual_name='amq.gen-old')
        self.old.queue_bind('amq.gen-old', 'events', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.BindOk())
        self.old.basic_consume('amq.gen-old',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))
        self.assertEqual(len(self.plan), 3)

        # The empty name addresses the most recent server-named queue.
        self.old.queue_delete('', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.DeleteOk())

        self.assertEqual(self.plan.operations, ())
        self.assertEqual(self.plan.generated_queue_names, ())

    def test_apply_replays_operations_in_order(self):
        self._record_full_topology()

        self._apply()
        # Only the first step is sent before its reply.
        sent = self._sent_on(self.new)
        self.assertEqual(len(sent), 2)  # Channel.Open, Exchange.Declare
        self.assertEqual(self.results, [])

        _deliver(self.connection, self.new, spec.Exchange.DeclareOk())
        sent = self._sent_on(self.new)
        self.assertEqual(len(sent), 3)
        self.assertIsInstance(sent[-1], spec.Queue.Declare)
        self.assertEqual(sent[-1].queue, 'orders')

        _deliver(self.connection, self.new,
                 spec.Queue.DeclareOk(queue='orders'))
        _deliver(self.connection, self.new, spec.Queue.BindOk())
        sent = self._sent_on(self.new)
        self.assertEqual([type(m) for m in sent[-2:]],
                         [spec.Queue.Bind, spec.Basic.Qos])
        self.assertEqual(sent[-2].routing_key, 'order.created')
        self.assertEqual(sent[-2].exchange, 'events')
        self.assertEqual(sent[-1].prefetch_count, 5)

        _deliver(self.connection, self.new, spec.Basic.QosOk())
        sent = self._sent_on(self.new)
        self.assertIsInstance(sent[-1], spec.Basic.Consume)
        self.assertEqual(sent[-1].queue, 'orders')
        self.assertEqual(sent[-1].consumer_tag, 'ctag')

        _deliver(self.connection, self.new, spec.Basic.ConsumeOk('ctag'))
        self.assertEqual(len(self.results), 1)
        result = self.results[0]
        self.assertTrue(result.succeeded)
        self.assertEqual(result.completed_stage, recovery.RecoveryStage.DONE)
        self.assertEqual(result.completed_operations, 5)
        self.assertIsNone(result.failed_stage)

    def test_generated_queue_name_is_replaced_on_recovery(self):
        self._declared('', actual_name='amq.gen-old')
        self.old.queue_bind('amq.gen-old', 'events', callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Queue.BindOk())
        self.old.basic_consume('amq.gen-old',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        _deliver(self.connection, self.old, spec.Basic.ConsumeOk('ctag'))
        self.assertEqual(self.plan.generated_queue_names, ('amq.gen-old',))

        self._apply()
        sent = self._sent_on(self.new)
        self.assertIsInstance(sent[-1], spec.Queue.Declare)
        self.assertEqual(sent[-1].queue, '')

        _deliver(self.connection, self.new,
                 spec.Queue.DeclareOk(queue='amq.gen-new'))
        sent = self._sent_on(self.new)
        self.assertIsInstance(sent[-1], spec.Queue.Bind)
        self.assertEqual(sent[-1].queue, 'amq.gen-new')
        self.assertEqual(self.plan.generated_queue_names, ('amq.gen-new',))

        _deliver(self.connection, self.new, spec.Queue.BindOk())
        sent = self._sent_on(self.new)
        self.assertIsInstance(sent[-1], spec.Basic.Consume)
        self.assertEqual(sent[-1].queue, 'amq.gen-new')

        _deliver(self.connection, self.new, spec.Basic.ConsumeOk('ctag'))
        self.assertTrue(self.results[0].succeeded)

    def test_failed_step_keeps_later_consumers_from_starting(self):
        self._declared()
        self.old.queue_bind('orders', 'events', callback=_confirm(None))
        self.old.basic_consume('orders',
                               mock.Mock(),
                               consumer_tag='ctag',
                               callback=_confirm(None))
        for method in [
                spec.Queue.BindOk(),
                spec.Basic.ConsumeOk(consumer_tag='ctag')
        ]:
            _deliver(self.connection, self.old, method)

        self._apply()
        _deliver(self.connection, self.new,
                 spec.Queue.DeclareOk(queue='orders'))
        sent = self._sent_on(self.new)
        self.assertIsInstance(sent[-1], spec.Queue.Bind)

        # The broker refuses the binding and closes the channel instead of
        # answering BindOk; the consumer step must not be sent.
        _deliver(self.connection, self.new,
                 spec.Channel.Close(406, 'PRECONDITION_FAILED', 0, 0))

        self.assertEqual(len(self.results), 1)
        result = self.results[0]
        self.assertFalse(result.succeeded)
        self.assertEqual(result.failed_stage, recovery.RecoveryStage.QUEUE_BIND)
        self.assertEqual(result.completed_stage,
                         recovery.RecoveryStage.QUEUE_DECLARE)
        self.assertEqual(result.completed_operations, 1)
        self.assertIsInstance(result.failure, exceptions.ChannelClosedByBroker)

        self.assertFalse(
            any(
                isinstance(method, spec.Basic.Consume)
                for method in self._sent_on(self.new)))

        # The record survives, so the caller can retry once the channel is replaced.
        self.assertEqual(len(self.plan), 3)

    def test_empty_plan_completes_immediately(self):
        self._apply()
        self.assertEqual(len(self.results), 1)
        self.assertTrue(self.results[0].succeeded)
        self.assertEqual(self.results[0].completed_operations, 0)
        self.assertEqual(self.results[0].completed_stage,
                         recovery.RecoveryStage.DONE)

    def test_apply_to_a_channel_that_never_opened_fails_at_begin(self):
        self._declared()
        closed = channel.Channel(self.connection, 3, lambda _chan: None)

        self.plan.apply(closed, self.results.append)

        self.assertEqual(len(self.results), 1)
        result = self.results[0]
        self.assertFalse(result.succeeded)
        self.assertEqual(result.failed_stage,
                         recovery.RecoveryStage.QUEUE_DECLARE)
        self.assertEqual(result.completed_stage, recovery.RecoveryStage.BEGIN)
        self.assertEqual(result.completed_operations, 0)
        self.assertIsInstance(result.failure, exceptions.ChannelWrongStateError)
        self.assertEqual(self._sent_on(closed), [])

    def test_declaring_a_known_name_twice_is_refused(self):
        self._declared()
        with self.assertRaises(ValueError):
            self.old.queue_declare('orders', callback=_confirm(None))
        # Refused before sending, so the record holds one declaration, not two.
        declarations = [
            method for method in self._sent_on(self.old)
            if isinstance(method, spec.Queue.Declare) and
            method.queue == 'orders' and not method.nowait
        ]
        self.assertEqual(len(declarations), 1)

    def test_a_second_plan_does_not_rerecord_older_operations(self):
        self._declared()
        replacement = recovery.ChannelRecoveryPlan()
        self.old.set_recovery_plan(replacement)
        self.assertEqual(replacement.operations, ())
        self.assertEqual(len(self.plan), 1)

    def test_channel_recovery_plan_property(self):
        self.assertIs(self.old.recovery_plan, self.plan)
        self.old.set_recovery_plan(None)
        self.assertIsNone(self.old.recovery_plan)
