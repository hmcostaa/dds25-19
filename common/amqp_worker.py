import json
import logging
from aio_pika import Message, connect_robust
from aio_pika.abc import (
    AbstractChannel, AbstractConnection,
    AbstractIncomingMessage
)


class AMQPWorker:
    connection: AbstractConnection
    channel: AbstractChannel

    def __init__(self, amqp_url: str, queue_name: str):
        """
        Initialize the AMQPConsumer.
        :param amqp_url: The RabbitMQ connection URL.
        :param queue_name: The name of the queue to consume messages from.
        """
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.callbacks = {}

    def register(self, callback):
        """
        Register a callback using the function's name as the message type.
        :param callback: The function to handle the message.
        """
        if not callable(callback):
            raise ValueError(f"Provided callback '{callback}' is not callable.")
        message_type = callback.__name__  # Use the function's name as the message type
        self.callbacks[message_type] = callback
        print(f"Registered callback for message type: '{message_type}'")

    async def start(self):
        """
        Start consuming messages from the queue.
        """
        self.connection = await connect_robust(self.amqp_url)
        async with self.connection:
            self.channel = await self.connection.channel()
            await self.channel.set_qos(prefetch_count=1)
            exchange = self.channel.default_exchange

            queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
            )

            async with queue.iterator() as qiterator:
                message: AbstractIncomingMessage
                async for message in qiterator:
                    try:
                        async with message.process():
                            # Decode the message body
                            data = json.loads(message.body.decode())
                            message_type = message.type

                            # Find the appropriate callback
                            if message_type in self.callbacks:
                                # TODO: callback_action in the callback function call
                                response = await self.callbacks[message_type](data, message.reply_to, message.correlation_id)
                                if response is not None:
                                    await exchange.publish(
                                        Message(
                                            body=json.dumps(response).encode(),
                                            correlation_id=message.correlation_id,
                                        ),
                                        routing_key=message.reply_to,
                                    )
                                print(f"Processed message of type '{message_type}'")
                                # TODO: Acknowledge that the message was processed
                            else:
                                logging.warning(f"No callback registered for message type '{message_type}'")
                    except Exception:
                        logging.exception("Processing error for message %r", message)

    async def send_message(self, payload, queue, correlation_id, reply_to, action=None, callback_action=None):
        exchange = self.channel.default_exchange
        await exchange.publish(
            Message(
                body=json.dumps(payload).encode(),
                correlation_id=correlation_id,
                type=action,
                reply_to=reply_to,
                callback_to=callback_action
            ),
            routing_key=queue,
        )