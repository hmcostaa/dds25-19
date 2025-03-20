import os
import json
import asyncio
import logging
import uuid
import time
from aio_pika import connect_robust, Message, DeliveryMode
from redis import Redis
from aiohttp import web, ClientSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration from environment variables
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
STOCK_SERVICE_URL = os.environ.get('STOCK_SERVICE_URL', 'http://stock-service:5000')
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


async def process_stock_request(message):
    """Process a stock request message."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')
        items = body.get('items', [])

        logger.info(f"Processing stock request for saga {saga_id}, order {order_id}")

        # Update saga state to indicate stock reservation is in progress
        update_saga_state(saga_id, "STOCK_RESERVATION_PROCESSING", {
            "order_id": order_id,
            "items": items
        })

        # TODO: Call stock service to reserve items
        # In a real implementation, you would make an HTTP request to the stock service
        # For each item in the order

        reserved_items = []
        async with ClientSession() as session:
            for item_id, quantity in items:
                try:
                    # Simulate stock reservation
                    await asyncio.sleep(0.5)

                    # Record the successful reservation for potential compensation
                    reserved_items.append((item_id, quantity))

                    # Update saga state with current progress
                    update_saga_state(saga_id, "STOCK_ITEM_RESERVED", {
                        "item_id": item_id,
                        "quantity": quantity
                    })

                except Exception as item_error:
                    logger.error(f"Failed to reserve item {item_id}: {str(item_error)}")

                    # Roll back previously reserved items
                    await rollback_stock_reservations(reserved_items, session)

                    # Update saga state to indicate failure
                    update_saga_state(saga_id, "STOCK_RESERVATION_FAILED", {
                        "item_id": item_id,
                        "error": str(item_error)
                    })

                    # Publish stock reservation failed event
                    await publish_event("stock.reservation_failed", {
                        "saga_id": saga_id,
                        "order_id": order_id,
                        "status": "failed",
                        "error": f"Failed to reserve item {item_id}: {str(item_error)}"
                    })

                    # Acknowledge message and exit
                    await message.ack()
                    return

        # All items successfully reserved
        update_saga_state(saga_id, "STOCK_RESERVATION_COMPLETED", {
            "reserved_items": reserved_items
        })

        # Publish stock reservation completed event
        await publish_event("stock.reservation_completed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "success",
            "reserved_items": reserved_items
        })

    except Exception as e:
        logger.error(f"Error processing stock reservation: {str(e)}")
        # Update saga state to indicate failure
        if saga_id:
            update_saga_state(saga_id, "STOCK_RESERVATION_FAILED", {"error": str(e)})

            # Publish stock reservation failed event
            await publish_event("stock.reservation_failed", {
                "saga_id": saga_id,
                "order_id": order_id if 'order_id' in locals() else None,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


async def process_stock_compensation(message):
    """Process a stock compensation request when another part of the saga fails."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')

        logger.info(f"Processing stock compensation for saga {saga_id}, order {order_id}")

        # Update saga state
        update_saga_state(saga_id, "STOCK_COMPENSATION_STARTED")

        # Get saga details to find which items were reserved
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            logger.error(f"Cannot find saga {saga_id} for compensation")
            await message.ack()
            return

        # Extract reserved items from saga state
        reserved_items = []
        for step in saga_data.get('steps', []):
            if step.get('status') == 'STOCK_RESERVATION_COMPLETED':
                details = step.get('details', {})
                if 'reserved_items' in details:
                    reserved_items = details['reserved_items']
                    break

        if not reserved_items:
            logger.info(f"No items to compensate for saga {saga_id}")
            update_saga_state(saga_id, "STOCK_COMPENSATION_COMPLETED", {"info": "No items to compensate"})
            await message.ack()
            return

        # Perform compensation (restore stock)
        async with ClientSession() as session:
            await rollback_stock_reservations(reserved_items, session)

        # Update saga state
        update_saga_state(saga_id, "STOCK_COMPENSATION_COMPLETED")

        # Publish compensation completed event
        await publish_event("stock.compensation_completed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "success"
        })

    except Exception as e:
        logger.error(f"Error processing stock compensation: {str(e)}")
        # Update saga state to indicate failure
        if 'saga_id' in locals():
            update_saga_state(saga_id, "STOCK_COMPENSATION_FAILED", {"error": str(e)})

            # Publish compensation failed event
            await publish_event("stock.compensation_failed", {
                "saga_id": saga_id,
                "order_id": order_id if 'order_id' in locals() else None,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


async def rollback_stock_reservations(items, session):
    """Roll back stock reservations by returning items to inventory."""
    for item_id, quantity in items:
        try:
            # In a real implementation, you would make an HTTP request to the stock service
            # to return the items to inventory
            await asyncio.sleep(0.2)  # Simulate API call
            logger.info(f"Rolled back reservation for item {item_id}, quantity {quantity}")
        except Exception as e:
            logger.error(f"Error rolling back reservation for item {item_id}: {str(e)}")
            # Continue with other items even if one fails


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


def get_saga_state(saga_id):
    """Get the current state of a saga from Redis."""
    try:
        saga_key = f"saga:{saga_id}"
        saga_json = saga_redis.get(saga_key)

        if saga_json:
            return json.loads(saga_json)
        else:
            logger.warning(f"Saga {saga_id} not found")
            return None

    except Exception as e:
        logger.error(f"Error getting saga state: {str(e)}")
        return None


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
                "leader:stock-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # 30 second expiration
            )

            if is_leader:
                logger.info("Acquired leadership")
            else:
                # Refresh leadership if already leader
                is_leader = saga_redis.get("leader:stock-orchestrator") == ORCHESTRATOR_ID
                if is_leader:
                    saga_redis.expire("leader:stock-orchestrator", 30)

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

            # Declare exchange and queues
            exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

            # Queue for stock reservation requests
            reservation_queue = await channel.declare_queue("stock_reservation_requests", durable=True)
            await reservation_queue.bind(exchange, "stock.reserve")

            # Queue for compensation requests
            compensation_queue = await channel.declare_queue("stock_compensation_requests", durable=True)
            await compensation_queue.bind(exchange, "stock.compensate")

            # Start consuming messages
            logger.info("Started consuming messages")
            await reservation_queue.consume(process_stock_request)
            await compensation_queue.consume(process_stock_compensation)

            # Keep the consumer running
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            await asyncio.sleep(5)  # Wait before reconnecting


async def recover_in_progress_sagas():
    """Recover any in-progress sagas that might have been abandoned."""
    try:
        # Find sagas in stock processing states
        for key in saga_redis.keys("saga:*"):
            saga_json = saga_redis.get(key)
            if saga_json:
                saga_data = json.loads(saga_json)

                # Check if saga is in a processing state and hasn't been updated recently
                if (saga_data.get("status") in ["STOCK_RESERVATION_PROCESSING", "STOCK_COMPENSATION_STARTED"] and
                        time.time() - saga_data.get("last_updated", 0) > 60):  # 60 second threshold

                    saga_id = saga_data.get("saga_id")
                    logger.info(f"Recovering saga {saga_id}")

                    # Update saga state to indicate recovery
                    update_saga_state(saga_id, "STOCK_RECOVERY_STARTED")

                    # For reservation processes, mark as failed to trigger compensation
                    if saga_data.get("status") == "STOCK_RESERVATION_PROCESSING":
                        update_saga_state(saga_id, "STOCK_RESERVATION_FAILED", {"error": "Recovered after timeout"})

                        # Publish event to notify other services
                        await publish_event("stock.reservation_failed", {
                            "saga_id": saga_id,
                            "order_id": saga_data.get("details", {}).get("order_id"),
                            "status": "failed",
                            "error": "Recovered after timeout"
                        })

                    # For compensation processes, try to complete them
                    elif saga_data.get("status") == "STOCK_COMPENSATION_STARTED":
                        # Extract order details
                        order_id = saga_data.get("details", {}).get("order_id")

                        # Re-trigger compensation process
                        await publish_event("stock.compensate", {
                            "saga_id": saga_id,
                            "order_id": order_id
                        })

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
    logger.info(f"Starting stock orchestrator (ID: {ORCHESTRATOR_ID})")

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