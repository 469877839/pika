"""
An optional record of broker-confirmed topology, replayed onto a new channel.

A channel is bound to its connection, so a reconnect needs new channels, and declaring the
exchanges, queues, bindings, QoS and consumers again is the caller's job. A `ChannelRecoveryPlan`
records what the broker confirmed on one channel, and `apply` replays that record, in order, onto a
channel the caller opens.

The plan is a record, not a supervisor: pika never reconnects on its own, and nothing is replayed
until the caller applies the plan. Publishing, acknowledgements and any declaration whose reply
never arrived are not part of the record.
"""

from __future__ import annotations

import dataclasses
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Union

if TYPE_CHECKING:
    from pika import channel

LOGGER = logging.getLogger(__name__)

_MessageCallback = Callable[..., Any]


class RecoveryStage(Enum):
    """A step of `ChannelRecoveryPlan.apply`."""

    BEGIN = 'begin'
    EXCHANGE_DECLARE = 'exchange_declare'
    QUEUE_DECLARE = 'queue_declare'
    QUEUE_BIND = 'queue_bind'
    QOS = 'qos'
    CONSUME = 'consume'
    DONE = 'done'


@dataclasses.dataclass
class ExchangeDeclareRecord:
    """An exchange declaration confirmed by the broker."""

    exchange: str
    exchange_type: str
    passive: bool
    durable: bool
    auto_delete: bool
    internal: bool
    arguments: dict[str, Any]


@dataclasses.dataclass
class QueueDeclareRecord:
    """
    A queue declaration confirmed by the broker.

    `queue` is the name the declaration asked for, which is empty when the broker generated one;
    `actual_name` is the name the broker answered with.
    """

    queue: str
    passive: bool
    durable: bool
    exclusive: bool
    auto_delete: bool
    arguments: dict[str, Any]
    actual_name: str = ''


@dataclasses.dataclass
class QueueBindRecord:
    """A queue binding confirmed by the broker."""

    queue: str
    exchange: str
    routing_key: str
    arguments: dict[str, Any]


@dataclasses.dataclass
class QosRecord:
    """A `Basic.Qos` confirmed by the broker."""

    prefetch_size: int
    prefetch_count: int
    global_qos: bool


@dataclasses.dataclass
class ConsumeRecord:
    """A consumer confirmed by the broker."""

    queue: str
    on_message_callback: _MessageCallback
    auto_ack: bool
    exclusive: bool
    consumer_tag: str
    arguments: dict[str, Any]


_TopologyRecord = Union[ExchangeDeclareRecord, QueueDeclareRecord,
                        QueueBindRecord, QosRecord, ConsumeRecord]


@dataclasses.dataclass
class RecoveryResult:
    """
    The outcome of `ChannelRecoveryPlan.apply`.

    `completed_stage` is the last stage that finished, or `RecoveryStage.BEGIN` when the run failed
    before completing a step. When `failure` is not `None`, `failed_stage` names the step that
    failed. The steps before it remain applied at the broker: a failure is not rolled back.
    """

    completed_stage: RecoveryStage
    failure: Exception | None
    failed_stage: RecoveryStage | None
    completed_operations: int

    @property
    def succeeded(self) -> bool:
        """Return whether the whole record was applied."""
        return self.failure is None


class ChannelRecoveryPlan:
    """
    A record of broker-confirmed channel topology, applied on demand to a new channel.

    Attach one plan to a channel with `Channel.set_recovery_plan`. The channel records what the
    broker confirms. A queue declared with an empty name keeps that empty request, and the bindings
    and consumers that refer to the generated queue are recorded under the name the broker answered,
    so a recovery that generates a fresh name rewrites those references to it. `queue_delete` and
    `basic_cancel` remove the corresponding records once the broker confirms them, and a consumer
    cancelled by the broker drops its record as well.

    `apply` replays the record in order and delivers a `RecoveryResult` to `on_done`. If the channel
    cannot send a step, that step fails immediately; a broker close fails the step in flight. A
    failure keeps the records, so a later attempt can retry, and it does not undo the broker's side
    of the steps that already completed.
    """

    def __init__(self) -> None:
        """Create an empty plan."""
        self._operations: list[_TopologyRecord] = []
        # For an explicit declaration, the asked name maps to itself. For a generated one, the
        # answered name maps to the name a later recovery generates for it.
        self._queue_names: dict[str, str] = {}
        # Answered names of empty-name declarations, in declaration order.
        self._generated_queues: list[str] = []
        # Most recent name the broker generated for an empty-name declaration.
        self._last_generated_name: str = ''

    @property
    def operations(self) -> tuple[_TopologyRecord, ...]:
        """Return the currently recorded operations, in replay order."""
        return tuple(self._operations)

    @property
    def generated_queue_names(self) -> tuple[str, ...]:
        """Return the answered names of empty-name declarations, in declaration order."""
        return tuple(self._generated_queues)

    def __len__(self) -> int:
        """Return the number of recorded operations."""
        return len(self._operations)

    def __bool__(self) -> bool:
        """Return whether the plan records at least one operation."""
        return bool(self._operations)

    def record_exchange_declare(self, exchange: str, exchange_type: str,
                                passive: bool, durable: bool, auto_delete: bool,
                                internal: bool, arguments: dict[str,
                                                                Any]) -> None:
        """Record a broker-confirmed `Exchange.Declare`."""
        self._operations.append(
            ExchangeDeclareRecord(exchange=exchange,
                                  exchange_type=exchange_type,
                                  passive=passive,
                                  durable=durable,
                                  auto_delete=auto_delete,
                                  internal=internal,
                                  arguments=arguments))

    def record_queue_declare(self, queue: str, passive: bool, durable: bool,
                             exclusive: bool, auto_delete: bool,
                             arguments: dict[str,
                                             Any], actual_name: str) -> None:
        """
        Record a broker-confirmed `Queue.Declare`, with its answered name.

        For a generated name the record keeps the empty request and the answered name becomes the
        target of later binding and consumer records.
        """
        self._operations.append(
            QueueDeclareRecord(queue=queue,
                               passive=passive,
                               durable=durable,
                               exclusive=exclusive,
                               auto_delete=auto_delete,
                               arguments=arguments,
                               actual_name=actual_name))
        if queue:
            self._queue_names[queue] = actual_name
        else:
            self._queue_names[actual_name] = actual_name
            self._generated_queues.append(actual_name)
            self._last_generated_name = actual_name

    def record_queue_bind(self, queue: str, exchange: str, routing_key: str,
                          arguments: dict[str, Any]) -> None:
        """Record a broker-confirmed `Queue.Bind`, resolving a generated queue name."""
        self._operations.append(
            QueueBindRecord(queue=self._resolve_queue_name(queue),
                            exchange=exchange,
                            routing_key=routing_key,
                            arguments=arguments))

    def record_qos(self, prefetch_size: int, prefetch_count: int,
                   global_qos: bool) -> None:
        """Record a broker-confirmed `Basic.Qos`."""
        self._operations.append(
            QosRecord(prefetch_size=prefetch_size,
                      prefetch_count=prefetch_count,
                      global_qos=global_qos))

    def record_consume(self, queue: str, on_message_callback: _MessageCallback,
                       auto_ack: bool, exclusive: bool, consumer_tag: str,
                       arguments: dict[str, Any]) -> None:
        """Record a broker-confirmed `Basic.Consume`, resolving a generated queue name."""
        self._operations.append(
            ConsumeRecord(queue=self._resolve_queue_name(queue),
                          on_message_callback=on_message_callback,
                          auto_ack=auto_ack,
                          exclusive=exclusive,
                          consumer_tag=consumer_tag,
                          arguments=arguments))

    def forget_exchange(self, exchange: str) -> None:
        """Remove every record that refers to a deleted exchange."""
        self._operations[:] = [
            operation for operation in self._operations
            if not (isinstance(operation, ExchangeDeclareRecord) and
                    operation.exchange == exchange)
        ]

    def _refuse_queue_name(self, queue: str) -> None:
        """Raise when a declaration reuses a name the plan already answered."""
        if queue and queue in self._queue_names:
            raise ValueError(
                f'queue {queue!r} is already declared in this recovery plan')

    def _resolve_queue_name(self, queue: str) -> str:
        """
        Return the name to record a binding or consumer under.

        Explicit names are kept. A declaration that asked the broker for a name answers with an
        empty request, so an empty reference means the most recent generated name.
        """
        if queue:
            return self._queue_names.get(queue, queue)
        if self._last_generated_name:
            return self._last_generated_name
        return ''

    def _repoint_generated_name(self, old_name: str, new_name: str) -> None:
        """
        Remap a name generated on the old channel to one generated on recovery.

        Records created after the first declaration answered use the generated name, so each one is
        rewritten to the fresh name and the alias map follows it.
        """
        for operation in self._operations:
            if (isinstance(operation,
                           (QueueDeclareRecord, QueueBindRecord, ConsumeRecord))
                    and operation.queue == old_name):
                operation.queue = new_name
            if (isinstance(operation, QueueDeclareRecord) and
                    operation.actual_name == old_name):
                operation.actual_name = new_name
        self._queue_names[old_name] = new_name
        self._generated_queues = [
            new_name if name == old_name else name
            for name in self._generated_queues
        ]
        if self._last_generated_name == old_name:
            self._last_generated_name = new_name

    def forget_queue(self, queue: str) -> None:
        """Remove the declaration and everything that referred to a deleted queue."""

        def belongs(operation: _TopologyRecord) -> bool:
            if not isinstance(
                    operation,
                (QueueDeclareRecord, QueueBindRecord, ConsumeRecord)):
                return False
            if operation.queue == queue:
                return True
            return (isinstance(operation, QueueDeclareRecord) and
                    operation.actual_name == queue)

        self._operations[:] = [
            operation for operation in self._operations
            if not belongs(operation)
        ]
        self._queue_names.pop(queue, None)
        if queue in self._generated_queues:
            self._generated_queues.remove(queue)
        if self._last_generated_name == queue:
            self._last_generated_name = ''

    def forget_consumer(self, consumer_tag: str) -> None:
        """Remove the record of a consumer the broker no longer serves."""
        self._operations[:] = [
            operation for operation in self._operations
            if not (isinstance(operation, ConsumeRecord) and
                    operation.consumer_tag == consumer_tag)
        ]

    def apply(self, channel: channel.Channel,
              on_done: Callable[[RecoveryResult], Any]) -> None:
        """
        Replay the recorded operations onto *channel*, in order.

        Each step is sent only after the broker confirms the previous one. *on_done* receives a
        `RecoveryResult`, once, when the record is exhausted or a step fails. A broker close of
        *channel* while a step is in flight fails that step and delivers the result. The method
        itself returns as soon as the first step is sent; it does not block.
        """
        _RecoveryRun(self, channel, on_done).start()


class _RecoveryRun:
    """One execution of a `ChannelRecoveryPlan`, advancing on each broker reply."""

    def __init__(self, plan: ChannelRecoveryPlan, channel: channel.Channel,
                 on_done: Callable[[RecoveryResult], Any]) -> None:
        """Create a run over *plan*'s current records, targeting *channel*."""
        # Snapshot the record: the plan may change while this run is in flight, and the run
        # reports on the steps it started.
        self._plan = plan
        self._operations = list(plan.operations)
        self._channel = channel
        self._on_done = on_done
        self._index = 0
        self._generated_index = 0
        self._completed_stage = RecoveryStage.BEGIN
        self._in_flight_stage = RecoveryStage.BEGIN
        self._finished = False
        channel.add_on_close_callback(self._on_channel_close)

    def start(self) -> None:
        """Send the first step, or complete an empty run."""
        self._advance()

    def _advance(self) -> None:
        """Send the next step, or report the result once all of them completed."""
        if self._finished:
            return
        if self._index == len(self._operations):
            self._finish(RecoveryStage.DONE, None, None)
            return
        operation = self._operations[self._index]
        stage = _stage_for(operation)
        try:
            if isinstance(operation,
                          QueueDeclareRecord) and not operation.queue:
                generated = self._plan.generated_queue_names
                old_name = (generated[self._generated_index]
                            if self._generated_index < len(generated) else '')
                self._generated_index += 1
                _send(self._channel, operation,
                      self._on_generated_queue_confirmed(old_name))
            else:
                _send(self._channel, operation, self._on_step_confirmed)
        except Exception as error:
            self._finish(self._completed_stage, stage, error)
            return
        self._in_flight_stage = stage

    def _on_generated_queue_confirmed(self,
                                      old_name: str) -> Callable[[Any], Any]:
        """Return a callback that repoints *old_name* at the broker's fresh name."""

        def on_confirmed(method_frame: Any) -> None:
            new_name = method_frame.method.queue
            if old_name and new_name != old_name:
                # The run's snapshot shares the plan's record objects, so the
                # repoint is visible to the steps this run has yet to send.
                self._plan._repoint_generated_name(old_name, new_name)
            self._on_step_confirmed(method_frame)

        return on_confirmed

    def _on_step_confirmed(self, _method_frame: Any) -> None:
        """Mark the in-flight step confirmed and send the next one."""
        if self._finished:
            return
        self._completed_stage = self._in_flight_stage
        self._index += 1
        self._advance()

    def _finish(self, completed_stage: RecoveryStage,
                failed_stage: RecoveryStage | None,
                failure: Exception | None) -> None:
        """Report the result and stop advancing."""
        if self._finished:
            return
        self._finished = True
        self._channel.remove_on_close_callback(self._on_channel_close)
        try:
            self._on_done(
                RecoveryResult(completed_stage=completed_stage,
                               failure=failure,
                               failed_stage=failed_stage,
                               completed_operations=self._index))
        except Exception:
            LOGGER.exception('Recovery completion callback raised')

    def _on_channel_close(self, _channel: channel.Channel,
                          reason: Exception) -> None:
        """Fail the in-flight step when the broker closes the channel."""
        if self._finished:
            return
        self._finish(self._completed_stage, self._in_flight_stage, reason)


def _stage_for(operation: _TopologyRecord) -> RecoveryStage:
    """Return the stage that replays *operation*."""
    if isinstance(operation, ExchangeDeclareRecord):
        return RecoveryStage.EXCHANGE_DECLARE
    if isinstance(operation, QueueDeclareRecord):
        return RecoveryStage.QUEUE_DECLARE
    if isinstance(operation, QueueBindRecord):
        return RecoveryStage.QUEUE_BIND
    if isinstance(operation, QosRecord):
        return RecoveryStage.QOS
    assert isinstance(operation, ConsumeRecord)
    return RecoveryStage.CONSUME


def _send(channel: channel.Channel, operation: _TopologyRecord,
          on_confirmed: Callable[[Any], Any]) -> None:
    """Send one recorded operation to *channel*, calling back *on_confirmed* on its reply."""
    if isinstance(operation, ExchangeDeclareRecord):
        channel.exchange_declare(exchange=operation.exchange,
                                 exchange_type=operation.exchange_type,
                                 passive=operation.passive,
                                 durable=operation.durable,
                                 auto_delete=operation.auto_delete,
                                 internal=operation.internal,
                                 arguments=operation.arguments,
                                 callback=on_confirmed)
    elif isinstance(operation, QueueDeclareRecord):
        channel.queue_declare(queue=operation.queue,
                              passive=operation.passive,
                              durable=operation.durable,
                              exclusive=operation.exclusive,
                              auto_delete=operation.auto_delete,
                              arguments=operation.arguments,
                              callback=on_confirmed)
    elif isinstance(operation, QueueBindRecord):
        channel.queue_bind(queue=operation.queue,
                           exchange=operation.exchange,
                           routing_key=operation.routing_key,
                           arguments=operation.arguments,
                           callback=on_confirmed)
    elif isinstance(operation, QosRecord):
        channel.basic_qos(prefetch_size=operation.prefetch_size,
                          prefetch_count=operation.prefetch_count,
                          global_qos=operation.global_qos,
                          callback=on_confirmed)
    else:
        assert isinstance(operation, ConsumeRecord)
        channel.basic_consume(queue=operation.queue,
                              on_message_callback=operation.on_message_callback,
                              auto_ack=operation.auto_ack,
                              exclusive=operation.exclusive,
                              consumer_tag=operation.consumer_tag,
                              arguments=operation.arguments,
                              callback=on_confirmed)
