import json
import logging
import uuid
import asyncio
import inspect
from aio_pika import Message, connect_robust, DeliveryMode
from aio_pika.abc import (
    AbstractChannel, AbstractConnection,
    AbstractIncomingMessage
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AMQPWorker:
    connection: AbstractConnection
    channel: AbstractChannel

    def __init__(self, amqp_url: str, queue_name: str, use_manual_acks: bool = True):
        """
        Initialize the AMQPConsumer.
        :param amqp_url: The RabbitMQ connection URL.
        :param queue_name: The name of the queue to consume messages from.
        """
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.callbacks = {}
        # self._use_manual_acks = use_manual_acks # possibility to use manual acks, still testing
        self._use_manual_acks = False
        logger.info(f"AMQPWorker queue name: '{queue_name}'")
        # logger.info(f"AMQPWorker queue name: '{queue_name}' Manual ACKs: {use_manual_acks}")

    def register(self, callback):
        """
        Register a callback using the function's name as the message type.
        :param callback: The function to handle the message.
        """
        if not callable(callback):
            raise ValueError(f"Provided callback '{callback}' is not callable.")
        
        #check for asyn func??
        message_type = callback.__name__  # Use the function's name as the message type
        self.callbacks[message_type] = callback
        logging.info(f"Registered callback for message type: '{message_type}'")


    #decooupled reply
    async def _handle_reply(self, message: AbstractIncomingMessage, response):
        if message.reply_to and response is not None:
            try:
                await self.channel.publish(
                    Message(
                        body=json.dumps(response).encode(),
                        correlation_id=message.correlation_id,
                        delivery_mode= DeliveryMode.PERSISTENT #persistent replies
                    ),
                    routing_key=message.reply_to,
                )
            except Exception as e:
                logging.error(f"Error publishing reply for message {message.correlation_id}: {str(e)}")

                            
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
            #manual ack
            if self._use_manual_acks:
                
                await queue.consume(self.on_message, no_ack=False)
                logger.info(" [*] Waiting for messages. To exit press CTRL+C")
                # worker rusns indefinitely
                await asyncio.Future()

            #auto ack, previous implementation
            else:
                async with queue.iterator() as qiterator:
                    message: AbstractIncomingMessage
                    async for message in qiterator:
                        async with message.process(requeue=False, ignore_processed=True):
                            response = None # Define before try block
                            try: 
                                # Decode the message body
                                data = json.loads(message.body.decode())
                                message_type = message.type

                                logger.debug(f" Received message type='{message_type}', "
                                         f"DeliveryTag={message.delivery_tag}, "
                                         f"CorrID='{message.correlation_id}'")
                                
                                # Find the appropriate callback
                                if message_type in self.callbacks:
                                    handler = self.callbacks[message_type]
                                    logger.debug(f"[AutoACK] Calling handler '{handler.__name__}'")

                                    #call handler with (data, message), now message included for idempotency
                                    response = await handler(data, message)

                                    await self._handle_reply(message, response)
                                else:
                                    logging.warning(f"No callback registered for message type '{message_type}'")
                            except Exception:
                                logging.exception("Processing error for message %r", message)
                                raise

    # async def on_message(self, message:AbstractIncomingMessage):
    #     """
    #     Manual ACK, for idempotency, still testing
    #     """
         
    #     response_tuple = None
    #     try:
    #         data = json.loads(message.body.decode())
    #         message_type = message.type
    #         logger.debug(f"[ManualACK] Received message type='{message_type}', DeliveryTag={message.delivery_tag}")

    #         if message_type in self.callbacks:
    #             handler = self.callbacks[message_type]
    #             logger.debug(f"[ManualACK] calling handler '{handler.__name__}'")
    #             # call handler with (data, message) 
    #             response_tuple = await handler(data, message)

    #             await self._handle_reply(message, response_tuple)

    #             # perform Manual ACK 
    #             logger.debug(f"[ManualACK] Acking message {message.delivery_tag}")
    #             await message.ack()

    #         else:
    #             logger.warning(f"[ManualACK] No callback registered for '{message_type}'. Rejecting message.")
    #             await message.reject(requeue=False) 

    #     except json.JSONDecodeError:
    #         try:
    #             await message.nack(requeue=False)
    #         except Exception as e:
    #             logger.exception("Error nacking message JSONDECODE")
    #     except Exception as e:
    #         logger.exception("Error in message processing loop")
    #         try:
    #             await message.nack(requeue=False)
    #         except Exception as e:
    #             logger.exception("Error nacking message UNKNOWN")


    async def send_message(self, payload, queue, correlation_id, reply_to, action=None, callback_action=None, attempt_id=None):
       

        if self.channel is None or self.channel.is_closed:
             logger.error("Cannot send message. Channel is closed")
             raise ConnectionError("AMQP Channel not available")
        
        exchange = self.channel.default_exchange
        
        if attempt_id:
             if isinstance(payload, dict):
                  payload['attempt_id'] = attempt_id
             else:
                  logger.warning("Cannot add attempt_id to message payload")

        #if correlation_id not provided, generate one\

        correlation_id = correlation_id or str(uuid.uuid4())

        await exchange.publish(
            Message(
                body=json.dumps(payload).encode(),
                correlation_id=correlation_id,
                type=action,
                reply_to=reply_to,
                callback_to=callback_action,
                delivery_mode=DeliveryMode.PERSISTENT #avpoids loss of messages, lower throughput, but reliable
            ),
            routing_key=queue,
        )
        # logger.debug(f"Sent message Action='{action}' CorrID='{correlation_id}' AttemptID='{attempt_id}' to queue '{queue}'")
        # return correlation_id, attempt_id
