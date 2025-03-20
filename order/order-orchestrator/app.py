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
ORDER_SERVICE_URL = os.environ.get('ORDER_SERVICE_URL', 'http://order-service:5000')
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


async def process_checkout_request(message):
    """Process an order checkout request and orchestrate the saga."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        order_id = body.get('order_id')

        logger.info(f"Processing checkout request for order {order_id}")

        # Create a new saga ID for this transaction
        saga_id = str(uuid.uuid4())

        # Initialize saga state
        update_saga_state(saga_id, "SAGA_STARTED", {
            "order_id": order_id,
            "initiated_at": time.time()
        })

        # Get order details from the order service
        async with ClientSession() as session:
            # In a real implementation, you would make an HTTP request to the order service
            # For simplicity, we'll use dummy data here
            # async with session.get(f"{ORDER_SERVICE_URL}/find/{order_id}") as response:
            #     if response.status != 200:
            #         raise Exception(f"Failed to get order details: {await response.text()}")
            #     order_data = await response.json()

            # Simulate getting order details
            await asyncio.sleep(0.5)
            order_data = {
                "order_id": order_id,
                "user_id": "user123",
                "items": [("item1", 2), ("item2", 1)],
                "total_cost": 100
            }

            # Update saga state with order details
            update_saga_state(saga_id, "ORDER_DETAILS_FETCHED", order_data)

            # Step 1: Reserve stock
            logger.info(f"Initiating stock reservation for saga {saga_id}")
            await publish_event("stock.reserve", {
                "saga_id": saga_id,
                "order_id": order_id,
                "items": order_data["items"]
            })

            # Note: The saga continues based on events from other services
            # We'll wait for stock.reservation_completed or stock.reservation_failed

    except Exception as e:
        logger.error(f"Error processing checkout request: {str(e)}")
        # If we have a saga_id, update the state
        if 'saga_id' in locals():
            update_saga_state(saga_id, "SAGA_FAILED_INITIALIZATION", {"error": str(e)})

            # Notify about the failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


async def process_stock_completed(message):
    """Process a stock reservation completed event and continue the saga."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')

        logger.info(f"Stock reservation completed for saga {saga_id}, order {order_id}")

        # Update saga state
        update_saga_state(saga_id, "STOCK_RESERVATION_COMPLETED")

        # Get saga details
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        # Extract order details
        order_data = saga_data.get("details", {})
        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        if not user_id or not total_cost:
            raise Exception("Missing user_id or total_cost in saga details")

        # Step 2: Process payment
        logger.info(f"Initiating payment for saga {saga_id}")
        await publish_event("payment.request", {
            "saga_id": saga_id,
            "order_id": order_id,
            "user_id": user_id,
            "amount": total_cost
        })

    except Exception as e:
        logger.error(f"Error processing stock completion: {str(e)}")
        # Update saga state
        if 'saga_id' in locals():
            update_saga_state(saga_id, "PAYMENT_INITIATION_FAILED", {"error": str(e)})

            # Compensate stock reservation
            await publish_event("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            })

            # Notify about failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


async def process_payment_completed(message):
    """Process a payment completed event and finalize the saga."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')

        logger.info(f"Payment completed for saga {saga_id}, order {order_id}")

        # Update saga state
        update_saga_state(saga_id, "PAYMENT_COMPLETED")

        # Step 3: Finalize order
        async with ClientSession() as session:
            # In a real implementation, you would make an HTTP request to the order service
            # For simplicity, we'll just simulate this
            # async with session.post(f"{ORDER_SERVICE_URL}/finalize/{order_id}") as response:
            #     if response.status != 200:
            #         raise Exception(f"Failed to finalize order: {await response.text()}")

            # Simulate finalizing order
            await asyncio.sleep(0.5)

            # Update saga state
            update_saga_state(saga_id, "ORDER_FINALIZED")
            update_saga_state(saga_id, "SAGA_COMPLETED", {
                "completed_at": time.time()
            })

            # Notify about successful checkout
            await publish_event("order.checkout_completed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "success"
            })

    except Exception as e:
        logger.error(f"Error finalizing order: {str(e)}")
        # Update saga state
        if 'saga_id' in locals():
            update_saga_state(saga_id, "ORDER_FINALIZATION_FAILED", {"error": str(e)})

            # Compensate payment and stock
            await publish_event("payment.reverse", {
                "saga_id": saga_id,
                "order_id": order_id
            })

            await publish_event("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            })

            # Notify about failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        # Acknowledge message
        await message.ack()


async def process_failure_events(message):
    """Process failure events from any part of the saga and initiate compensation."""
    try:
        # Parse message body
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')
        error = body.get('error', 'Unknown error')
        routing_key = message.routing_key

        logger.info(f"Processing failure event {routing_key} for saga {saga_id}")

        # Update saga state
        update_saga_state(saga_id, f"FAILURE_EVENT_{routing_key.upper()}", {"error": error})

        # Get saga details to determine compensation actions
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            logger.error(f"Cannot find saga {saga_id} for compensation")
            await message.ack()
            return

        # Determine what steps have completed based on saga history
        steps_completed = [step["status"] for step in saga_data.get("steps", [])]

        # Compensate completed steps in reverse order
        compensations = []

        if "PAYMENT_COMPLETED" in steps_completed and routing_key != "payment.failed":
            compensations.append(("payment.reverse", {
                "saga_id": saga_id,
                "order_id": order_id
            }))

        if "STOCK_RESERVATION_COMPLETED" in steps_completed and routing_key != "stock.reservation_failed":
            compensations.append(("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            }))

        # Execute compensations
        for routing_key, payload in compensations:
            await publish_event(routing_key, payload)

        # Update saga state to failed
        update_saga_state(saga_id, "SAGA_FAILED", {
            "error": error,
            "compensations_initiated": [c[0] for c in compensations],
            "failed_at": time.time()
        })

        # Notify about checkout failure
        await publish_event("order.checkout_failed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "failed",
            "error": error
        })

    except Exception as e:
        logger.error(f"Error processing failure event: {str(e)}")
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
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # 30 second expiration
            )

            if is_leader:
                logger.info("Acquired leadership")
            else:
                # Refresh leadership if already leader
                is_leader = saga_redis.get("leader:order-orchestrator") == ORCHESTRATOR_ID
                if is_leader:
                    saga_redis.expire("leader:order-orchestrator", 30)

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

            # Queue for checkout requests
            checkout_queue = await channel.declare_queue("order_checkout_requests", durable=True)
            await checkout_queue.bind(exchange, "order.checkout")

            # Queue for stock completion events
            stock_complete_queue = await channel.declare_queue("stock_complete_events", durable=True)
            await stock_complete_queue.bind(exchange, "stock.reservation_completed")

            # Queue for payment completion events
            payment_complete_queue = await channel.declare_queue("payment_complete_events", durable=True)
            await payment_complete_queue.bind(exchange, "payment.completed")

            # Queue for failure events
            failure_queue = await channel.declare_queue("saga_failure_events", durable=True)
            await failure_queue.bind(exchange, "stock.reservation_failed")
            await failure_queue.bind(exchange, "payment.failed")

            # Start consuming messages
            logger.info("Started consuming messages")
            await checkout_queue.consume(process_checkout_request)
            await stock_complete_queue.consume(process_stock_completed)
            await payment_complete_queue.consume(process_payment_completed)
            await failure_queue.consume(process_failure_events)

            # Keep the consumer running
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error consuming messages: {str(e)}")
            await asyncio.sleep(5)  # Wait before reconnecting


async def recover_in_progress_sagas():
    """Recover any in-progress sagas that might have been abandoned."""
    try:
        # Find sagas in processing states that haven't been updated recently
        for key in saga_redis.keys("saga:*"):
            saga_json = saga_redis.get(key)
            if saga_json:
                saga_data = json.loads(saga_json)

                # Ignore completed or failed sagas
                if saga_data.get("status") in ["SAGA_COMPLETED", "SAGA_FAILED"]:
                    continue

                # Check if saga hasn't been updated recently
                if time.time() - saga_data.get("last_updated", 0) > 60:  # 60 second threshold
                    saga_id = saga_data.get("saga_id")
                    order_id = saga_data.get("details", {}).get("order_id")

                    logger.info(f"Recovering saga {saga_id} for order {order_id}")

                    # Update saga state
                    update_saga_state(saga_id, "SAGA_RECOVERY_STARTED")

                    # For simplicity, just mark the saga as failed and trigger compensations
                    update_saga_state(saga_id, "SAGA_FAILED_RECOVERY", {
                        "error": "Recovered after timeout",
                        "recovered_at": time.time()
                    })

                    # Determine what steps have completed
                    steps_completed = [step["status"] for step in saga_data.get("steps", [])]

                    # Trigger appropriate compensations
                    if "PAYMENT_COMPLETED" in steps_completed:
                        await publish_event("payment.reverse", {
                            "saga_id": saga_id,
                            "order_id": order_id
                        })

                    if "STOCK_RESERVATION_COMPLETED" in steps_completed:
                        await publish_event("stock.compensate", {
                            "saga_id": saga_id,
                            "order_id": order_id
                        })

                    # Notify about failure
                    await publish_event("order.checkout_failed", {
                        "saga_id": saga_id,
                        "order_id": order_id,
                        "status": "failed",
                        "error": "Recovered after timeout"
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
    logger.info(f"Starting order orchestrator (ID: {ORCHESTRATOR_ID})")

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