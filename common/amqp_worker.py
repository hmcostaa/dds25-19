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
        logger.info(f"AMQPWorker queue name: '{queue_name}'")

    def register(self, callback):
        """
        Register a callback using the function's name as the message type.
        :param callback: The function to handle the message.
        """
        if not callable(callback):
            raise ValueError(f"Provided callback '{callback}' is not callable.")
        
        #check for asyn func??
        is_async = inspect.iscoroutinefunction(callback)
        message_type = callback.__name__ 
        self.callbacks[message_type] = callback
        logging.info(f"Registered callback for message type: '{message_type}'")

    async def _handle_reply(self, message: AbstractIncomingMessage, response):
        correlation_id = message.correlation_id or str(uuid.uuid4())
        reply_body_dict = {} 
        final_status_code = 500 

        if isinstance(response, tuple) and len(response) == 2 and isinstance(response[1], int):
            data_or_error_dict = response[0]
            final_status_code = response[1]
            if isinstance(data_or_error_dict, dict):
                if "error" in data_or_error_dict:
                    reply_body_dict["error"] = data_or_error_dict["error"] 
                else:
                    reply_body_dict["data"] = data_or_error_dict

            else:
                reply_body_dict["error"] = "Worker returned invalid data format"
                final_status_code = 500
            reply_body_dict["status_code"] = final_status_code 
        else:
            logger.warning(f"Handler for msg type '{message.type}' returned unexpected format: {type(response)}. CorrID: {correlation_id}")
            reply_body_dict["error"] = "Internal worker error: Unexpected response format"
            reply_body_dict["status_code"] = 500 

        if message.reply_to:
            try:
                json_body = json.dumps(reply_body_dict).encode("utf-8")
                logging.info(f"--- WORKER HANDLE REPLY: Sending structured reply for CorrID {correlation_id}: {reply_body_dict}")

                await self.channel.default_exchange.publish(
                    Message(
                        body=json_body,
                        correlation_id=correlation_id,
                        content_type="application/json", 
                        delivery_mode=DeliveryMode.PERSISTENT
                    ),
                    routing_key=message.reply_to,
                )

                logger.info(f"Reply for message {correlation_id} sent successfully")
            
            except Exception as e:
                logger.error(f"Error publishing structured reply for message {correlation_id}: {str(e)}", exc_info=True)
        elif not message.reply_to:
            
            logger.warning(f"No reply_to queue specified for CorrID {correlation_id}, reply not sent.")

    async def start(self):
        logger.info(f"Starting AMQPWorker for queue: {self.queue_name}")
        try:
            self.connection = await connect_robust(self.amqp_url)
            
            async with self.connection:
                self.channel = await self.connection.channel()
                
                await self.channel.set_qos(prefetch_count=1)

                dlx_name = f"{self.queue_name}.dlx"
                dlq_name = f"{self.queue_name}.dlq"
                dlq_routing_key = dlq_name

                logger.info(f"Setting up dead letter exchange: {dlx_name} and queue: {dlq_name}")
                dlx = await self.channel.declare_exchange(
                    dlx_name,
                    type="direct",
                    durable=True,
                )
                logger.debug(f"DLX '{dlx_name}' declared successfully")

                dlq = await self.channel.declare_queue(
                    dlq_name,
                    durable=True,
                )
                logger.debug(f"DLQ '{dlq_name}' declared successfully")

                logger.info(f"Declared DLX '{dlx_name}' and DLQ '{dlq_name}'")

                logger.debug(f"Binding DLQ '{dlq_name}' to DLX '{dlx_name}' with routing key '{dlq_routing_key}'")
                await dlq.bind(exchange=dlx, routing_key=dlq_routing_key)
                logger.debug(f"DLQ binding completed")
                
                main_queue_arguments = {
                    "x-dead-letter-exchange": dlx_name,
                    "x-dead-letter-routing-key": dlq_routing_key
                }
                logger.debug(f"Main queue arguments: {main_queue_arguments}")
                

                logger.info(f"Declaring main queue: {self.queue_name}")
                queue = await self.channel.declare_queue(
                    self.queue_name,
                    durable=True,
                    arguments=main_queue_arguments
                )
                logger.debug(f"Main queue declared with arguments: {main_queue_arguments}")

                logger.info(f"Declared queue '{self.queue_name}' linked to DLX '{dlx_name}'")
                
                # #manual ack
                # if self._use_manual_acks:
                logger.info(f"Starting to consume from queue: {self.queue_name} with manual acknowledgment")
                await queue.consume(self.on_message, no_ack=False)
                
                await asyncio.Future()
        except Exception as e:
            logger.critical(f"Failed to start AMQPWorker: {str(e)}", exc_info=True)
            raise



    async def on_message(self, message:AbstractIncomingMessage):
        """
        Manual ACK, for idempotency, still testing
        """
        correlation_id = message.correlation_id or "no-correlation-id"
        response_tuple = None
        handler = None
        message_type = None
        handler_succeeded = False
        reply_succeeded = False

        #decode
        print(f"--- AMQP: on_message - Received message type='{message.type}', DeliveryTag={message.delivery_tag} ---")
        try:
            try:
                data = json.loads(message.body.decode('utf-8'))
                message_type = message.type
                print(f"--- AMQP: on_message - Received message type='{message_type}', DeliveryTag={message.delivery_tag} ---")

                if not message_type:
                    print(f"--- AMQP: on_message - Message type not found-")
                    await message.nack(requeue=False)
                    return
                handler = self.callbacks[message_type]
                logger.debug(f"AMQP found handler '{handler.__name__} for message type '{message_type}'")

            except json.JSONDecodeError as e:
                logger.error(f"failed to decode JSON. error={e}, corrID={message.correlation_id}, DeliveryTag={message.delivery_tag}", exc_info=True)
                await message.nack(requeue=False)
                return
            except KeyError as e:
                logger.error(f" No handler found for message type '{message_type}'")
                await message.nack(requeue=False)
                return
            except Exception as e:
                logger.critical(f"Unexpected error in on_message: {e}")
                await message.nack(requeue=False)
                return


            #execute handler
            try:
                print(f"--- AMQP_WORKER: Before calling handler {handler.__name__} for DeliveryTag {message.delivery_tag}")
                response_tuple = await handler(data, message)
                handler_succeeded = True
                print(f"--- AMQP_WORKER: After calling handler {handler.__name__} for DeliveryTag {message.delivery_tag}")
                logger.info(f"Handler {handler.__name__} for message type '{message_type}' executed successfully")

            except (redis.RedisError, asyncio.TimeoutError, IdempotencyStoreConnectionError, ConnectionError) as e:
                logger.warning(f"Transient error DeliveryTag={message.delivery_tag} Error={str(e)}")
                handler_succeeded = False
            except Exception as e:
                logger.error(f"Fatal error DeliveryTag={message.delivery_tag} Type={type(e)} Error={str(e)}", exc_info=True)
                handler_succeeded = False

            if handler_succeeded and message.reply_to:
                try:
                    await self._handle_reply(message, response_tuple)
                    reply_succeeded = True
                    logger.info(f"Reply for message {message.correlation_id} sent successfully")
                except Exception as e:
                    logger.error(f"Error handling reply for message {message.correlation_id}: {str(e)}")
                    reply_succeeded = False
            elif handler_succeeded and not message.reply_to:
                reply_succeeded = True #no was reply needed
                logger.info(f"Handler {handler.__name__} for message type '{message_type}' executed successfully")
    
        except Exception as e:
            logger.critical(f"Unexpected error in on_message: {e}")
            handler_succeeded = False
            reply_succeeded = False
        
        finally:
            print(f"--- AMQP_WORKER: on_message - Finally ---")
            try:
                if handler_succeeded and reply_succeeded:
                    await message.ack()
                    logger.info(f"Message {message.correlation_id} processed successfully")
                else:
                    reason = "Handler failed" if not handler_succeeded else "Reply failed" if not reply_succeeded else "Unknown"
                    logger.warning(f"Message {message.correlation_id} failed: {reason}")
                    await message.nack(requeue=False)
            except Exception as e:
                logger.critical(f"Unexpected error in on_message: {e}", exc_info=True)
                #not really a resolveable error
    

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

        try:
            message = Message(
                body=json.dumps(payload).encode(),
                correlation_id=correlation_id,
                type=action,
                reply_to=reply_to,
                delivery_mode=DeliveryMode.PERSISTENT #avpoids loss of messages, lower throughput, but reliable
            )
            logger.debug(f"Created message object with delivery_mode=PERSISTENT")
            
            await exchange.publish(
                message,
                routing_key=queue,
            )
            logger.info(f"Message published successfully to queue '{queue}' with correlation_id '{correlation_id}'")
        except Exception as e:
            logger.error(f"Failed to publish message to '{queue}': {str(e)}", exc_info=True)
            raise


