# Recovery Plan

A reconnect replaces the channel, so the exchange and queue declarations, bindings, QoS and consumers on the old channel have to be established again. A `ChannelRecoveryPlan` records the operations the broker confirmed on one channel and replays them, in order, onto a new channel when the caller applies it.

The plan is opt-in and a record rather than a supervisor. Pika does not reconnect on its own, does not replay `basic_publish`, `basic_ack` or a declaration whose reply never arrived, and leaves the connection open or closed exactly as the caller made it. Attach a plan to a channel with `Channel.set_recovery_plan`, open a replacement channel after reconnecting, and call `ChannelRecoveryPlan.apply` with an `on_done` callback that receives a `RecoveryResult`.

```python
plan = pika.recovery.ChannelRecoveryPlan()

# On the original channel.
channel.set_recovery_plan(plan)
channel.exchange_declare('events', 'topic', durable=True, callback=on_ok)
channel.queue_declare('orders', durable=True, callback=on_ok)
channel.queue_bind('orders', 'events', 'order.created', callback=on_ok)
channel.basic_qos(prefetch_count=10, callback=on_ok)
channel.basic_consume('orders', on_message, consumer_tag='orders-1', callback=on_ok)

# After reconnecting and opening a new channel.
def on_recovered(result):
    if not result.succeeded:
        # result.failed_stage names the step that failed; earlier steps stay
        # applied at the broker and are not rolled back.
        log.warning('recovery stopped at %s: %r', result.failed_stage,
                    result.failure)

new_channel.set_recovery_plan(plan)
plan.apply(new_channel, on_recovered)
```

Only waited, broker-confirmed operations enter the record: a nowait call has no reply and is not recorded, and `queue_delete` and `basic_cancel` remove the matching records once the broker confirms them. A consumer the broker cancels on its own is removed as well.

A queue declared with an empty name is server-generated, so its name changes when it is declared again. The plan keeps the declaration under the empty request and rewrites the queue name its later bindings and consumers use, so they reference the name the recovered broker answered with rather than the old one.

A failed step delivers a `RecoveryResult` whose `failed_stage` names the step and whose `completed_stage` names the last step that finished. The steps before it remain applied at the broker; Pika does not claim to have rolled anything back, and the record is kept so the caller can retry on another channel.

::: pika.recovery
    options:
      members: true
      show_if_no_docstring: true
