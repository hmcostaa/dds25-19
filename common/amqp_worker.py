import json
import logging
import uuid
import asyncio
import inspect
import redis
from global_idempotency.app import IdempotencyStoreConnectionError
from aio_pika import Message, connect_robust, DeliveryMode
from aio_pika.abc import (
    AbstractChannel, AbstractConnection,
    AbstractIncomingMessage
)

logging.basicConfig(level=logging.DEBUG)
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
        # self._use_manual_acks = use_manual_acks
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
        correlation_id = message.correlation_id or str(uuid.uuid4())
        if message.reply_to and response is not None:
            try:
                await self.channel.default_exchange.publish(
                    Message(
                        body=json.dumps(response).encode(),
                        correlation_id=correlation_id,
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

            dlx_name = f"{self.queue_name}.dlx"
            dlq_name = f"{self.queue_name}.dlq"
            dlq_routing_key = dlq_name

            await self.channel.declare_exchange(
                dlx_name,
                type="direct",
                durable=True,
            )

            await self.channel.declare_queue(
                dlq_name,
                durable=True,
            )

            logger.info(f"Declared DLX '{dlx_name}' and DLQ '{dlq_name}'")

            await self.channel.queue.bind(
                queue = dlq_name,
                exchange = dlx_name,
                routing_key = dlq_routing_key
            )
            
            main_queue_arguments = {
                "x-dead-letter-exchange": dlx_name,
                "x-dead-letter-routing-key": dlq_routing_key
            }
            

            queue = await self.channel.declare_queue(
                self.queue_name,
                durable=True,
                arguments=main_queue_arguments
            )

            logger.info(f"Declared queue '{self.queue_name}' linked to DLX '{dlx_name}'")
            
            # #manual ack
            # if self._use_manual_acks:
            await queue.consume(self.on_message, no_ack=False)
            logger.info(" [*] Waiting for messages. To exit press CTRL+C")
            await asyncio.Future()


    async def on_message(self, message:AbstractIncomingMessage):
        """
        Manual ACK, for idempotency, still testing
        """

        response_tuple = None
        handler = None
        message_type = None
        processed_successfully = False
        ack_status = "pending"

        try:
            data = json.loads(message.body.decode())
            message_type = message.type
            logger.debug(f" Received message type='{message_type}', DeliveryTag={message.delivery_tag}")

            if not message_type in self.callbacks:
                ack_status = "nacked_dlq"
                return
            try:
                handler = self.callbacks[message_type]
                logger.debug(f"calling handler '{handler.__name__}'")
                # call handler with (data, message) 
                response_tuple = await handler(data, message)

                processed_successfully = True
                logger.info("Reply processed successfully")

            except (redis.RedisError, asyncio.TimeoutError, IdempotencyStoreConnectionError, ConnectionError) as e:
                logger.warning(f"Transient error DeliveryTag={message.delivery_tag} Error={str(e)}")
                ack_status = "nacked_dlq"
            
            except Exception as e:
                logger.error(f"Fatal error DeliveryTag={message.delivery_tag} Error={str(e)}")
                ack_status = "nacked_dlq"
                #potential rpc timeout custom rpc timeout
                #transient errors

        # except json.JSONDecodeError as e:
        #     logger.error(f"JSONDecodeError DeliveryTag={message.delivery_tag} Error={str(e)}")
        #     requeue = False
        # except Exception as e:
        #     logger.exception("Error in message processing loop DeliverTag=%s", message.delivery_tag)
        #     requeue = False

        #     #retry/DlQ TODO
        #     #potentially requeue for some errors??
        #     #for now --> just assume processing error -> no requeue

        #     # try:
        #     #     await message.nack(should_requeue)
        #     #     logger.info("Message nacked DeliverTag=%s", message.delivery_tag)
        #     # except Exception as e:
        #     #     logger.exception("Error nacking message UNKNOWN")
        #     # return
        # try:
            if processed_successfully:
                try:
                    await self._handle_reply(message, response_tuple)
                    logger.info("Message processed successfully")
                except Exception as e:
                    logger.error(f"Error handling reply for message {message.correlation_id}: {str(e)}")
        
        except Exception as e:
            logger.critical("unexpected critical error, DeliverTag=%s", message.delivery_tag)

        finally:
            try:
                if ack_status == "pending":
                    if processed_successfully:
                        await message.ack()
                        ack_status = "acked"
                        logger.debug("Message acked DeliverTag=%s", message.delivery_tag)
                    else:
                        logger.warning("finally: Message processing failed, DeliverTag=%s", message.delivery_tag)
                        await message.nack(requeue=True)
                        ack_status = "nacked_dlq"

                elif ack_status == "nacked_dlq":
                    await message.nack(requeue=False)
                    logger.debug("Message nacked DeliverTag=%s", message.delivery_tag)

                elif ack_status == "acked":
                    pass

            except Exception as e:
                #failed to ack/nack after processing, TERRIBLE

                logger.critical(f"Error acking message, processing succceeded but ack failed, DeliverTag={message.delivery_tag}")

        #not necessary because of the deadletter queue
        # else:
        #     #processing failed, weird case, since exceptions are handled above
        #     logger.warning("Message processing failed, DeliverTag=%s", message.delivery_tag)
        #     try:
        #         await message.nack(requeue=False)
        #     except Exception as e:
        #         logger.exception("Error in fallback nack for DeliverTag=%s", message.delivery_tag)



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

        await exchange.default_exchange.publish(
            Message(
                body=json.dumps(payload).encode(),
                correlation_id=correlation_id,
                type=action,
                reply_to=reply_to,
                delivery_mode=DeliveryMode.PERSISTENT #avpoids loss of messages, lower throughput, but reliable
            ),
            routing_key=queue,
        )
        # logger.debug(f"Sent message Action='{action}' CorrID='{correlation_id}' AttemptID='{attempt_id}' to queue '{queue}'")
        # return correlation_id, attempt_id
