import os
import json
import asyncio
import logging
import uuid
import time

# Import Order Service
from order.app import find_order, acquire_write_lock, release_write_lock

# RabbitMQ library
from aio_pika import connect_robust, Message, DeliveryMode

# RPC client for internal service calls
from common.rpc_client import RpcClient

# Redis library
from redis import Redis

# HTTP client and server (for optional health checks or external calls)
from aiohttp import web, ClientSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize RPC client
rpc_client = RpcClient()

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------
AMQP_URL = os.environ.get('AMQP_URL', 'amqp://user:password@rabbitmq:5672/')
ORDER_SERVICE_URL = os.environ.get('ORDER_SERVICE_URL', 'http://order-service:5000')

# Redis saga state
SAGA_REDIS_HOST = os.environ.get('SAGA_REDIS_HOST', 'redis-saga-master')
SAGA_REDIS_PORT = int(os.environ.get('SAGA_REDIS_PORT', 6379))
SAGA_REDIS_PASSWORD = os.environ.get('SAGA_REDIS_PASSWORD', 'redis')
SAGA_REDIS_DB = int(os.environ.get('SAGA_REDIS_DB', 0))

# Initialize Redis client
saga_redis = Redis(
    host=SAGA_REDIS_HOST,
    port=SAGA_REDIS_PORT,
    password=SAGA_REDIS_PASSWORD,
    db=SAGA_REDIS_DB,
    decode_responses=True
)

# Generate a unique ID for this orchestrator instance
ORCHESTRATOR_ID = str(uuid.uuid4())

# ----------------------------------------------------------------------------
# 1. Process an order checkout request (starting point of the saga)
# ----------------------------------------------------------------------------
async def process_checkout_request(message):
    """
    Receives 'order.checkout' events to begin the saga.
    Tries to reserve stock first, then waits for success/failure events.
    """
    try:
        body = json.loads(message.body.decode())
        order_id = body.get('order_id')
        logger.info(f"[Order Orchestrator] Received checkout request for order {order_id}")

        # Create a new Saga ID for this checkout
        saga_id = str(uuid.uuid4())

        # Initialize saga state
        update_saga_state(saga_id, "SAGA_STARTED", {
            "order_id": order_id,
            "initiated_at": time.time()
        })

        # (Optional) fetch order details from your Order Service
        # async with ClientSession() as session:
        #     # Example: this might be an HTTP GET to fetch details
        #     # resp = await session.get(f"{ORDER_SERVICE_URL}/find/{order_id}")
        #     # order_data = await resp.json()
        #     # For demonstration, we’ll just simulate an order payload:
        #     await asyncio.sleep(0.3)  # simulating network/processing
        #     order_data = {
        #         "order_id": order_id,
        #         "user_id": "some_user",
        #         "items": [("item1", 2), ("item2", 1)],
        #         "total_cost": 999
        #     }

        order_data = await find_order(order_id)
        
        # Lock order to prevent concurrent processing
        order_lock_value = acquire_write_lock(order_id)
        if order_lock_value is None:
            raise Exception("Order is already being processed")
        logger.info(f"[Order Orchestrator] Lock acquired {order_lock_value} for order {order_id}")
        update_saga_state(saga_id, "ORDER_LOCKED", order_data)

        # Step 1: Publish "stock.reserve" so the Stock Service can reserve items
        logger.info(f"[Order Orchestrator] Sending stock.reserve for saga={saga_id}, order={order_id}")
        await publish_event("stock.reserve", {
            "saga_id": saga_id,
            "order_id": order_id,
            "items": order_data["items"]
        })

        # TODO: Pay for the order if stock reservation is successful

        # TODO: Subtract stock from inventory if payment is successful

        # TODO: Release the order lock after the saga completes

    except Exception as e:
        logger.error(f"Error in process_checkout_request: {str(e)}")
        if 'saga_id' in locals():
            # Mark the saga as failed
            update_saga_state(saga_id, "SAGA_FAILED_INITIALIZATION", {"error": str(e)})
            # Notify order of failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        await message.ack()

# ----------------------------------------------------------------------------
# 2. Handle stock reservation completion
# ----------------------------------------------------------------------------
async def process_stock_completed(message):
    """
    Receives 'stock.reservation_completed' event after Stock Service reserves items.
    Then we proceed to request payment.
    """
    try:
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')

        logger.info(f"[Order Orchestrator] Stock reservation completed for saga={saga_id}, order={order_id}")
        update_saga_state(saga_id, "STOCK_RESERVATION_COMPLETED")

        # Retrieve saga state to get user_id, total_cost, etc.
        saga_data = get_saga_state(saga_id)
        if not saga_data:
            raise Exception(f"Saga {saga_id} not found")

        order_data = saga_data.get("details", {})
        user_id = order_data.get("user_id")
        total_cost = order_data.get("total_cost")

        if not user_id or not total_cost:
            raise Exception("Missing user_id or total_cost in saga details")

        # Step 2: Publish "payment.request" for Payment Service
        logger.info(f"[Order Orchestrator] Sending payment.request for saga={saga_id}, order={order_id}")
        await publish_event("payment.request", {
            "saga_id": saga_id,
            "order_id": order_id,
            "user_id": user_id,
            "amount": total_cost
        })

    except Exception as e:
        logger.error(f"Error in process_stock_completed: {str(e)}")
        if 'saga_id' in locals():
            update_saga_state(saga_id, "PAYMENT_INITIATION_FAILED", {"error": str(e)})
            # Request stock compensation since we can’t proceed
            await publish_event("stock.compensate", {
                "saga_id": saga_id,
                "order_id": order_id
            })
            # Notify order of failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        await message.ack()

# ----------------------------------------------------------------------------
# 3. Handle payment completion
# ----------------------------------------------------------------------------
async def process_payment_completed(message):
    """
    Receives 'payment.completed' after the Payment Service finishes charging.
    Then the order can be finalized.
    """
    try:
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')

        logger.info(f"[Order Orchestrator] Payment completed for saga={saga_id}, order={order_id}")
        update_saga_state(saga_id, "PAYMENT_COMPLETED")

        # Step 3: Finalize the order in your Order Service
        async with ClientSession() as session:
            # e.g. finalize call to your order service
            # resp = await session.post(f"{ORDER_SERVICE_URL}/finalize/{order_id}")
            # ...
            await asyncio.sleep(0.3)  # simulating an HTTP call

        update_saga_state(saga_id, "ORDER_FINALIZED")
        update_saga_state(saga_id, "SAGA_COMPLETED", {"completed_at": time.time()})

        # Notify success
        await publish_event("order.checkout_completed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "success"
        })

    except Exception as e:
        logger.error(f"Error in process_payment_completed: {str(e)}")
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
            # Notify failure
            await publish_event("order.checkout_failed", {
                "saga_id": saga_id,
                "order_id": order_id,
                "status": "failed",
                "error": str(e)
            })
    finally:
        await message.ack()

# ----------------------------------------------------------------------------
# 4. Handle generic failure events
#    (e.g., if Stock or Payment explicitly publish "reservation_failed" or "payment.failed")
# ----------------------------------------------------------------------------
async def process_failure_events(message):
    """
    Any service can publish a failure event: e.g. 'stock.reservation_failed' or 'payment.failed'.
    The orchestrator listens, updates the saga state, and triggers compensation if needed.
    """
    try:
        body = json.loads(message.body.decode())
        saga_id = body.get('saga_id')
        order_id = body.get('order_id')
        error = body.get('error', 'Unknown error')
        routing_key = message.routing_key

        logger.info(f"[Order Orchestrator] Failure event {routing_key} for saga={saga_id}: {error}")
        update_saga_state(saga_id, f"FAILURE_EVENT_{routing_key.upper()}", {"error": error})

        saga_data = get_saga_state(saga_id)
        if not saga_data:
            logger.error(f"[Order Orchestrator] Saga {saga_id} not found.")
            await message.ack()
            return

        # Check which steps were completed
        steps_completed = [step["status"] for step in saga_data.get("steps", [])]
        compensations = []

        # If payment was completed but we got a failure from somewhere else, reverse it
        if "PAYMENT_COMPLETED" in steps_completed and routing_key != "payment.failed":
            compensations.append(("payment.reverse", {"saga_id": saga_id, "order_id": order_id}))

        # If stock was reserved but we have a new failure (not from stock reservation itself), roll it back
        if "STOCK_RESERVATION_COMPLETED" in steps_completed and routing_key != "stock.reservation_failed":
            compensations.append(("stock.compensate", {"saga_id": saga_id, "order_id": order_id}))

        # Publish compensation events in reverse order
        for comp_key, payload in compensations:
            await publish_event(comp_key, payload)

        update_saga_state(saga_id, "SAGA_FAILED", {
            "error": error,
            "compensations_initiated": [c[0] for c in compensations],
            "failed_at": time.time()
        })

        # Notify that checkout failed
        await publish_event("order.checkout_failed", {
            "saga_id": saga_id,
            "order_id": order_id,
            "status": "failed",
            "error": error
        })

    except Exception as e:
        logger.error(f"Error in process_failure_events: {str(e)}")
    finally:
        await message.ack()

# ----------------------------------------------------------------------------
# Saga State Helpers
# ----------------------------------------------------------------------------
def update_saga_state(saga_id, status, details=None):
    """
    Store/Update the saga state in Redis with the latest step.
    """
    try:
        saga_key = f"saga:{saga_id}"
        saga_json = saga_redis.get(saga_key)

        if saga_json:
            saga_data = json.loads(saga_json)
        else:
            # Create a new saga record if not found
            saga_data = {
                "saga_id": saga_id,
                "status": status,
                "steps": [],
                "created_at": time.time()
            }

        saga_data["status"] = status
        saga_data["last_updated"] = time.time()

        if details:
            saga_data["details"] = {**saga_data.get("details", {}), **details}

        # Append a step entry
        saga_data["steps"].append({
            "status": status,
            "timestamp": time.time(),
            "details": details
        })

        saga_redis.set(saga_key, json.dumps(saga_data))
        logger.info(f"[Order Orchestrator] Saga {saga_id} updated -> {status}")
    except Exception as e:
        logger.error(f"Error updating saga state: {str(e)}")

def get_saga_state(saga_id):
    """Retrieve the saga's current state from Redis."""
    try:
        saga_key = f"saga:{saga_id}"
        saga_json = saga_redis.get(saga_key)
        if saga_json:
            return json.loads(saga_json)
        logger.warning(f"[Order Orchestrator] Saga {saga_id} not found.")
        return None
    except Exception as e:
        logger.error(f"Error getting saga state: {str(e)}")
        return None

# ----------------------------------------------------------------------------
# RabbitMQ Publisher
# ----------------------------------------------------------------------------
async def publish_event(routing_key, payload):
    """
    Publishes a message to the “saga_events” exchange with the given routing_key.
    """
    try:
        connection = await connect_robust(AMQP_URL)
        channel = await connection.channel()

        # Declare exchange of type "topic" (or direct/fanout as needed)
        exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

        message = Message(
            body=json.dumps(payload).encode(),
            delivery_mode=DeliveryMode.PERSISTENT
        )

        await exchange.publish(message, routing_key=routing_key)
        logger.info(f"[Order Orchestrator] Published event {routing_key} => {payload}")

        await connection.close()
    except Exception as e:
        logger.error(f"Error publishing event: {str(e)}")

# ----------------------------------------------------------------------------
# Leadership (Optional) and Recovery
# ----------------------------------------------------------------------------
async def maintain_leadership():
    """
    Optional: For high availability, tries to maintain “leadership” so only one
    instance runs the saga orchestration at a time. If not needed, remove.
    """
    while True:
        try:
            # Attempt to acquire leadership
            is_leader = saga_redis.set(
                "leader:order-orchestrator",
                ORCHESTRATOR_ID,
                nx=True,
                ex=30  # expires in 30 seconds
            )
            if is_leader:
                logger.info("[Order Orchestrator] Acquired leadership.")
            else:
                # If already the leader, refresh the TTL
                if saga_redis.get("leader:order-orchestrator") == ORCHESTRATOR_ID:
                    saga_redis.expire("leader:order-orchestrator", 30)

            # Heartbeat
            saga_redis.setex(f"heartbeat:{ORCHESTRATOR_ID}", 30, "alive")
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Leadership maintenance error: {str(e)}")
            await asyncio.sleep(5)

async def recover_in_progress_sagas():
    """
    If there are sagas stuck in a “processing” state for too long,
    mark them as failed or attempt compensation.
    """
    try:
        for key in saga_redis.keys("saga:*"):
            saga_json = saga_redis.get(key)
            if not saga_json:
                continue

            saga_data = json.loads(saga_json)
            status = saga_data.get("status")
            last_updated = saga_data.get("last_updated", 0)

            # Check for sagas that appear 'stuck'
            if status not in ["SAGA_COMPLETED", "SAGA_FAILED"]:
                if time.time() - last_updated > 60:  # 60s threshold
                    saga_id = saga_data.get("saga_id")
                    order_id = saga_data.get("details", {}).get("order_id")
                    logger.warning(f"[Order Orchestrator] Found stuck saga={saga_id}, forcing fail.")

                    update_saga_state(saga_id, "SAGA_FAILED_RECOVERY", {
                        "error": "Timeout recovery triggered"
                    })

                    # Trigger compensation if needed
                    steps_completed = [step["status"] for step in saga_data.get("steps", [])]
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
                    # Notify that it failed
                    await publish_event("order.checkout_failed", {
                        "saga_id": saga_id,
                        "order_id": order_id,
                        "status": "failed",
                        "error": "Timeout recovery triggered"
                    })

    except Exception as e:
        logger.error(f"Error recovering sagas: {str(e)}")

# ----------------------------------------------------------------------------
# RabbitMQ Consumer Setup
# ----------------------------------------------------------------------------
async def consume_messages():
    """
    Connect to RabbitMQ, declare exchange/queues, and consume relevant events.
    """
    while True:
        try:
            connection = await connect_robust(AMQP_URL)
            channel = await connection.channel()

            # Declare exchange
            exchange = await channel.declare_exchange("saga_events", "topic", durable=True)

            # Declare & bind relevant queues
            checkout_queue = await channel.declare_queue("order_checkout_requests", durable=True)
            await checkout_queue.bind(exchange, routing_key="order.checkout")

            stock_complete_queue = await channel.declare_queue("stock_complete_events", durable=True)
            await stock_complete_queue.bind(exchange, routing_key="stock.reservation_completed")

            payment_complete_queue = await channel.declare_queue("payment_complete_events", durable=True)
            await payment_complete_queue.bind(exchange, routing_key="payment.completed")

            failure_queue = await channel.declare_queue("saga_failure_events", durable=True)
            await failure_queue.bind(exchange, routing_key="stock.reservation_failed")
            await failure_queue.bind(exchange, routing_key="payment.failed")

            # Start consumers
            logger.info("[Order Orchestrator] Consuming messages...")
            await checkout_queue.consume(process_checkout_request)
            await stock_complete_queue.consume(process_stock_completed)
            await payment_complete_queue.consume(process_payment_completed)
            await failure_queue.consume(process_failure_events)

            # Keep the consumer alive
            await asyncio.Future()

        except Exception as e:
            logger.error(f"Error in consume_messages: {str(e)}")
            await asyncio.sleep(5)

# ----------------------------------------------------------------------------
# Optional Health Check
# ----------------------------------------------------------------------------
async def health_check_server():
    """
    Runs a simple HTTP server on port 8000 to respond with 'OK' for readiness checks.
    """
    async def health_handler(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/health", health_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host="0.0.0.0", port=8000)
    await site.start()
    logger.info("[Order Orchestrator] Health check server started on :8000")

# ----------------------------------------------------------------------------
# Main Entry Point
# ----------------------------------------------------------------------------
async def main():
    logger.info(f"Starting Order Orchestrator (ID={ORCHESTRATOR_ID})")

    # Health check server (optional)
    asyncio.create_task(health_check_server())

    # Leadership election (optional)
    asyncio.create_task(maintain_leadership())

    # Attempt to recover any stuck sagas on startup
    await recover_in_progress_sagas()

    # Start consuming saga messages
    await consume_messages()

if __name__ == "__main__":
    asyncio.run(main())
