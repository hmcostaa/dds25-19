import json
import uuid
import asyncio
from typing import MutableMapping
import logging
from aio_pika import Message, connect_robust
from aio_pika.abc import (
    AbstractChannel, AbstractConnection,
    AbstractIncomingMessage, AbstractQueue
)

logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

class RpcClient:
    connection: AbstractConnection
    channel: AbstractChannel
    callback_queue: AbstractQueue

    def __init__(self) -> None:
        self.futures: MutableMapping[str, asyncio.Future] = {}

    async def connect(self, url: str) -> "RpcClient":
        try:
            self.connection = await connect_robust(url)
            self.channel = await self.connection.channel()
            queue_arguments={
                "x-dead-letter-exchange": "dead-letter-exchange",
                "x-dead-letter-routing-key": "dead_letter"
            }
            self.callback_queue = await self.channel.declare_queue(
                exclusive=True,
                durable=True,
                arguments=queue_arguments)
            await self.callback_queue.consume(self.on_response, no_ack=True)

            return self
        except Exception as e:
            logger.exception(f"Unexpected error connecting to AMQP server: {e}")

    async def on_response(self, message: AbstractIncomingMessage) -> None:
        if message.correlation_id is None:
            print(f"Bad message {message!r}")
            return

        future: asyncio.Future = self.futures.pop(message.correlation_id, None)
        if future:
            try:
                decoded_body = json.loads(message.body.decode())
                logging.info(f"--- RPC CLIENT ON_RESPONSE: Setting future result for CorrID {message.correlation_id}. Result: {decoded_body}")
                future.set_result(decoded_body)
            except Exception as e:
                logging.exception(f"--- RPC CLIENT ON_RESPONSE: Error decoding/setting future result for CorrID {message.correlation_id}: {e}")
                future.set_exception(e)
        else:
            logging.info(f"--- RPC CLIENT ON_RESPONSE: Successfully set future result for CorrID {message.correlation_id}")
            

    async def call(self, queue: str, action: str, payload: object = None) -> object:
        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()

        print(f"--- RPC CLIENT CALL: Sending action '{action}' to queue '{queue}'. CorrID: {correlation_id}, Payload: {payload}")

        correlation_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        self.futures[correlation_id] = future

        body = json.dumps(payload if payload is not None else {}).encode()

        await self.channel.default_exchange.publish(
            Message(
                body=body,
                content_type="application/json",
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
                type=action,
            ),
            routing_key=queue,
        )

        logger.info(f"--- RPC CLIENT CALL: Message published for CorrID {correlation_id}. Waiting for future...")
        result = await future
        print(f"--- RPC CLIENT CALL: Future resolved for CorrID {correlation_id}. Result: {result}")
        return result

    async def call_stock(self, action: str, data: dict):
        return await self.call("stock_queue", action, data)

    async def call_payment(self, action: str, data: dict):
        return await self.call("payment_queue", action, data)
