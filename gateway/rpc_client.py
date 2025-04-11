import json
import uuid
import asyncio
from typing import MutableMapping
from aio_pika import Message, connect_robust
from aio_pika.abc import (
    AbstractChannel, AbstractConnection,
    AbstractIncomingMessage, AbstractQueue
)


class RpcClient:
    connection: AbstractConnection
    channel: AbstractChannel
    callback_queue: AbstractQueue

    def __init__(self) -> None:
        self.futures: MutableMapping[str, asyncio.Future] = {}

    async def connect(self, url: str) -> "RpcClient":
        self.connection = await connect_robust(url)
        self.channel = await self.connection.channel()
        queue_arguments={
            "x-dead-letter-exchange": "dead-letter-exchange",
            "x-dead-letter-routing-key": "dead_letter"
        }
        self.callback_queue = await self.channel.declare_queue(name='',
            exclusive=True,
            durable=True, arguments=queue_arguments)
        await self.callback_queue.consume(self.on_response, no_ack=True)

        return self

    async def on_response(self, message: AbstractIncomingMessage) -> None:
        if message.correlation_id is None:
            print(f"Bad message {message!r}")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id)
        future.set_result(json.loads(message.body.decode()))

    async def call(self, queue: str, action: str, payload: object = None) -> object:
        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                json.dumps(payload).encode(),
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
                type=action,
            ),
            routing_key=queue,
        )

        return await future

    async def call_stock(self, action: str, data: dict):
        return await self.call("stock_queue", action, data)

    async def call_payment(self, action: str, data: dict):
        return await self.call("payment_queue", action, data)
