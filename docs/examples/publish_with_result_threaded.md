# Thread-Safe Publish with a Routing Result

`basic_publish` is fire-and-forget, and a `Basic.Ack` alone does not prove a message reached a queue: an unroutable `mandatory=True` message is answered with `Basic.Return` *and then* `Basic.Ack`. `Channel.publish_with_result` tracks one message through the return, the ack/nack and any channel failure, and reports a single terminal `PublishStatus` per caller-supplied `message_id`.

```python
result = ch.publish_with_result(
    message_id='order-1234',   # unique while in flight
    exchange='orders',
    routing_key='new',
    body=b'...',
    mandatory=True,            # required
)
result.wait(timeout=5)
result.status                 # ROUTED / NOT_ROUTED / NACKED / CHANNEL_FAILED
```

- `message_id` is written into `properties.message_id`, because `Basic.Return` carries no delivery tag. Reusing an ID that is still in flight raises `ValueError`.
- Publisher confirms are enabled automatically; a `confirm_delivery` listener still receives every confirm.
- `mandatory=True` is enforced, because without it an unroutable message is silently dropped and the ack would be indistinguishable from a routed one.
- `wait(timeout=...)` timing out and `cancel_wait()` cancel only the wait: the message was already sent and stays tracked, so its terminal result still arrives and can be retrieved by a later `wait()`.
- `max_inflight_publishes` on `Connection` caps the unresolved set; the next publish then raises `pika.exceptions.InFlightPublishLimitError` before writing any frame.
- A channel or connection close resolves every unresolved message as `CHANNEL_FAILED` (or `NOT_ROUTED` if it had already been returned), so a blocked `wait` cannot outlive the connection.

`examples/publish_with_result_threaded.py`:

```python
--8<-- "examples/publish_with_result_threaded.py"
```
