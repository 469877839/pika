# Channel

## Closing a channel with active consumers

A `Basic.Cancel` does not stop the broker immediately. The specification allows it to keep dispatching for a cancelled consumer until it sends `Basic.CancelOk`, so there is a window in which further deliveries arrive for a consumer you have already cancelled. Pika passes those deliveries to the consumer callback, which is what the RabbitMQ Java client does.

!!! warning

    `Channel.close()` cancels each consumer without waiting for that confirmation, so any delivery the broker had already dispatched is discarded rather than passed to the callback. A callback could not acknowledge one anyway: the channel is closing and refuses the frame. A discarded delivery is redelivered to the next consumer when `auto_ack=False`, and **lost when `auto_ack=True`**, because an `auto_ack` delivery has no copy left at the broker. Pika logs a warning naming the count as the channel closes.

    This applies to `channel.close()` on `SelectConnection`, `AsyncioConnection`, `TornadoConnection` and `GeventConnection`. [`pika.Connection`](adapters/thread_safe.md) and [`BlockingConnection`](adapters/blocking.md) are unaffected, because both cancel their consumers and wait for the broker to confirm before closing anything.

To close a channel on one of the asynchronous adapters without losing those deliveries, cancel each consumer with a callback and close from the last `Basic.CancelOk`:

```python
def stop_consuming(channel, consumer_tags):
    """Close `channel` once the broker has confirmed every cancellation."""
    pending = set(consumer_tags)

    def on_cancelok(method_frame):
        pending.discard(method_frame.method.consumer_tag)
        if not pending:
            channel.close()

    for consumer_tag in consumer_tags:
        channel.basic_cancel(consumer_tag, callback=on_cancelok)
```

Each delivery that arrives before its `Basic.CancelOk` reaches the consumer callback, so a callback written on the assumption that cancelling is immediate may still run, and may still acknowledge.

Pika 2.0 removes the asynchronous adapters, leaving [`Connection`](adapters/thread_safe.md) and its `Channel` as the only close path, so the gap is retired rather than fixed.

## Deadlines for synchronous RPCs

Synchronous methods such as `queue_declare` wait for a reply from the broker, and by default they wait forever. Each of them accepts a `deadline` argument, a number of seconds to wait for its own reply once the request has been sent. The default, `None`, keeps the previous behavior.

AMQP 0.9.1 replies carry no request identifier, so a reply that arrives after its deadline cannot be told apart from the reply to an identical request issued later. Rather than risk matching a straggler to the wrong call, the channel takes the safe side when a deadline expires:

- The expired request's callbacks are detached, so its late reply completes no call.
- Requests queued behind it are discarded without being sent.
- Further synchronous RPCs on the channel raise `ChannelRpcTimeout` instead of being sent.
- Fire-and-forget methods that expect no reply (`basic_publish`, `basic_ack`, ...) remain usable.
- `Channel.close()` tears the channel down locally without sending `Channel.Close`, whose `Channel.CloseOk` could not be told apart from the overdue reply either. Open a new channel to continue.

The request was already on the wire when the deadline expired, so the operation **may have taken effect on the broker** even though its reply never arrived. Verify the outcome out of band (for example with a passive `queue_declare` on a fresh channel) before retrying an operation that is not idempotent.

On `BlockingConnection`, the same `deadline` argument makes the blocking call raise `ChannelRpcTimeout` to its caller when it expires.

::: pika.channel

## Channel

::: pika.channel.Channel
    options:
      members: true
      inherited_members: true
      members_order: source
