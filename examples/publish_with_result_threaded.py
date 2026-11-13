"""
Example: publish with a routing result using Connection.

basic_publish() is fire-and-forget and a Basic.Ack alone does not prove the message reached a
queue: an unroutable mandatory message is answered with Basic.Return and then Basic.Ack.
Channel.publish_with_result() tracks one message through the return, the ack/nack and any channel
failure and reports a single terminal PublishStatus per caller-supplied message_id.

No broker is needed to understand the flow; run this against a reachable RabbitMQ to see it live.
"""

import logging

import pika
from pika.adapters.thread_safe_connection import PublishStatus

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

EXCHANGE_NAME = 'test_exchange'
ROUTABLE_KEY = 'routable_key'
UNROUTABLE_KEY = 'no_binding_here'


def main():
    parameters = pika.ConnectionParameters(
        credentials=pika.PlainCredentials('guest', 'guest'))

    LOGGER.info('Connecting ...')
    conn = pika.Connection(parameters, max_inflight_publishes=16)
    ch = conn.channel()

    # Publish a routable and an unroutable mandatory message.  Each call
    # enables publisher confirms on first use and returns immediately; wait()
    # blocks until the terminal state is known.
    for message_id, routing_key in (
        ('order-1', ROUTABLE_KEY),
        ('order-2', UNROUTABLE_KEY),
    ):
        result = ch.publish_with_result(
            message_id=message_id,
            exchange=EXCHANGE_NAME,
            routing_key=routing_key,
            body=f'payload for {message_id}'.encode(),
            properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent),
        )

        # A timeout does not withdraw the message; it stays tracked and the
        # same handle can be waited on again.  cancel_wait() has the same
        # meaning for a wait that must stop early.
        result.wait(timeout=10)

        if result.status is PublishStatus.ROUTED:
            LOGGER.info('%s reached at least one queue', message_id)
        elif result.status is PublishStatus.NOT_ROUTED:
            LOGGER.info('%s was returned: %s %s', message_id,
                        result.reply_code, result.reply_text)
        elif result.status is PublishStatus.NACKED:
            LOGGER.warning('%s was nacked by the broker', message_id)
        else:
            LOGGER.error('%s failed before an outcome: %r', message_id,
                         result.exception)

    conn.close()
    LOGGER.info('Done')


if __name__ == '__main__':
    main()
