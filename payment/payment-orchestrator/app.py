import os
import json
import asyncio
import logging
import uuid
import time
from aio_pika import connect_robust, Message, DeliveryMode
from aiohttp import web
from redis import Redis

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
PAYMENT_SERVICE_URL = os.environ.get('PAYMENT_SERVICE_URL', 'http://payment-service:5000')
SAGA_REDIS_HOST = os.environ.get('SAGA_REDIS_HOST', 'redis-saga-master')
SAGA_REDIS_PORT = int(os.environ.get('SAGA_REDIS_PORT', 6379))
SAGA_REDIS_PASSWORD = os.environ.get('SAGA_REDIS_PASSWORD', 'redis')
SAGA_REDIS_DB = int(os.environ.get('SAGA_REDIS_DB', 0))

# Initialize Redis client for saga state
saga_redis = Redis(
    host=SAGA_REDIS_HOST,
    port=SAGA_REDIS_PORT,
    password=SAGA_REDIS_PASSWORD,
    db=SAGA_REDIS_DB,
    decode_responses=True  # For easier JSON handling
)

# Generate unique ID for this orchestrator instance
ORCHESTRATOR_ID = str(uuid.uuid4())


async def process_payment_request(message):
    """Process a payment request message."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')
        user_id = body.get('user_id')
        amount = body.get('amount')

        logger.info(f"Processing payment request for saga {saga_id}, order {order_id}")

        # Update saga state to indicate payment is in progress
        update_saga_state(saga_id, "PAYMENT_PROCESSING", {
            "order_id": order_id,
            "user_id": user_id,
            "amount": amount
        })

        # TODO: Call payment service to process payment
        # In a real implementation, you would make an HTTP request to the payment service
        # For demo purposes, we'll simulate success

        # Simulate payment processing
        await asyncio.sleep(1)

        # Update saga state to indicate payment is complete
        update_saga_state(saga_id, "PAYMENT_COMPLETED")

        # Publish payment completed event
        await publish_event("payment.completed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "success"
        })

    except Exception as e:
        logger.error(f"Error processing payment: {str(e)}")
        # Update saga state to indicate payment failed
        if saga_id:
            update_saga_state(saga_id, "PAYMENT_FAILED", {"error": str(e)})

            # Publish payment failed event
            await publish_event("payment.failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


def update_saga_state(saga_id, status, details=None):
    """Update the state of a saga in Redis."""
    try:
        # Get current saga state
        saga_key = f"saga:{saga_id}"
        saga_json = saga_redis.get(saga_key)

        if saga_json:
            saga_data = json.loads(saga_json)
        else:
            # Create new saga state if it doesn't exist
            saga_data = {
                "saga_id": saga_id,
                "status": "STARTED",
                "steps": [],
                "created_at": time.time()
            }

        # Update saga status and details
        saga_data["status"] = status
        saga_data["last_updated"] = time.time()

        if details:
            saga_data["details"] = {**saga_data.get("details", {}), **details}

        # Add step to history
        saga_data["steps"].append({
            "status": status,
            "timestamp": time.time(),
            "details": details
        })

        # Save updated saga state
        saga_redis.set(saga_key, json.dumps(saga_data))
        logger.info(f"Updated saga {saga_id} state to {status}")

    except Exception as e:
        logger.error(f"Error updating saga state: {str(e)}")


async def publish_event(routing_key, payload):
    """Publish an event to RabbitMQ."""
    try:
        connection = await connect_robust(AMQP_URL)
        channel = await connection.channel()

        # Declare exchange
        exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

        # Create message with the event payload
        message = Message(
            body=json.dumps(payload).encode(),
            delivery_mode=DeliveryMode.PERSISTENT
        )

        # Publish message
        await exchange.publish(message, routing_key=routing_key)
        logger.info(f"Published event {routing_key}")

        # Close connection
        await connection.close()

    except Exception as e:
        logger.error(f"Error publishing event: {str(e)}")


async def maintain_leadership():
    """Maintain leadership or acquire it if possible."""
    while True:
        try:
            # Try to acquire leadership
            is_leader = saga_redis.set(
                "leader:payment-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # 30 second expiration
            )

            if is_leader:
                logger.info("Acquired leadership")
            else:
                # Refresh leadership if already leader
                is_leader = saga_redis.get("leader:payment-orchestrator") == ORCHESTRATOR_ID
                if is_leader:
                    saga_redis.expire("leader:payment-orchestrator", 30)

            # Update heartbeat
            saga_redis.setex(
                f"heartbeat:{ORCHESTRATOR_ID}",
                30,
                "alive"
            )

            await asyncio.sleep(10)  # Check every 10 seconds

        except Exception as e:
            logger.error(f"Error in leadership maintenance: {str(e)}")
            await asyncio.sleep(5)  # Shorter interval on error


async def consume_messages():
    """Consume messages from RabbitMQ."""
    while True:
        try:
            # Connect to RabbitMQ
            connection = await connect_robust(AMQP_URL)
            channel = await connection.channel()

            # Declare exchange and queue
            exchange = await channel.declare_exchange("saga_events", "topic", durable=True)
            queue = await channel.declare_queue("payment_requests", durable=True)

            # Bind queue to exchange with routing key
            await queue.bind(exchange, "payment.request")

            # Start consuming messages
            logger.info("Started consuming messages")
            await queue.consume(process_payment_request)

            # Keep the consumer running
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            await asyncio.sleep(5)  # Wait before reconnecting


async def recover_in_progress_sagas():
    """Recover any in-progress sagas that might have been abandoned."""
    try:
        # Find sagas in PAYMENT_PROCESSING state
        for key in saga_redis.keys("saga:*"):
            saga_json = saga_redis.get(key)
            if saga_json:
                saga_data = json.loads(saga_json)

                # Check if saga is in a processing state and hasn't been updated recently
                if (saga_data.get("status") == "PAYMENT_PROCESSING" and
                        time.time() - saga_data.get("last_updated", 0) > 60):  # 60 second threshold

                    saga_id = saga_data.get("saga_id")
                    logger.info(f"Recovering saga {saga_id}")

                    # Update saga state to indicate recovery
                    update_saga_state(saga_id, "PAYMENT_RECOVERY_STARTED")

                    # TODO: Implement recovery logic
                    # This would involve checking the actual state with the payment service
                    # and then either completing or rolling back the payment

                    # For now, we'll just mark it as failed to trigger compensation
                    update_saga_state(saga_id, "PAYMENT_FAILED", {"error": "Recovered after timeout"})

    except Exception as e:
        logger.error(f"Error recovering sagas: {str(e)}")


async def health_check_server():
    """Run a simple HTTP server for health checks."""


    async def health_handler(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get('/health', health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8000)
    await site.start()
    logger.info("Health check server started on port 8000")


async def main():
    """Main entry point for the orchestrator."""
    logger.info(f"Starting payment orchestrator (ID: {ORCHESTRATOR_ID})")

    # Start health check server
    asyncio.create_task(health_check_server())

    # Start leadership maintenance
    asyncio.create_task(maintain_leadership())

    # Recover any in-progress sagas
    await recover_in_progress_sagas()

    # Start consuming messages
    await consume_messages()


if __name__ == "__main__":
    asyncio.run(main())