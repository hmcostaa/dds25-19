import json
import logging
from aio_pika import Message, connect_robust
from aio_pika.abc import AbstractIncomingMessage


class AMQPWorker:
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
        connection = await connect_robust(self.amqp_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=1)
            exchange = channel.default_exchange

            queue = await channel.declare_queue(
                self.queue_name,
                durable=True,
            )

            async with queue.iterator() as qiterator:
                message: AbstractIncomingMessage
                async for message in qiterator:
                    try:
                        async with message.process():
                            # Decode the message body
                            payload = json.loads(message.body.decode())
                            message_type = payload.get("type")
                            data = payload.get("data")

                            # Find the appropriate callback
                            if message_type in self.callbacks:
                                response = await self.callbacks[message_type](data)
                                # Publish the response back to the reply queue
                                await exchange.publish(
                                    Message(
                                        body=json.dumps(response).encode(),
                                        correlation_id=message.correlation_id,
                                    ),
                                    routing_key=message.reply_to,
                                )
                                print(f"Processed message of type '{message_type}'")
                            else:
                                logging.warning(f"No callback registered for message type '{message_type}'")
                    except Exception:
                        logging.exception("Processing error for message %r", message)
