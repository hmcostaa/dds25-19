import os
import json
import uuid
import time
import asyncio
import logging
import warnings
import sys
from msgspec import msgpack

warnings.filterwarnings("ignore")

from order.app import (
get_order_from_db,acquire_write_lock,release_write_lock,OrderValue)

from aio_pika import connect_robust, Message, DeliveryMode

# RPC client for internal service calls
from common.rpc_client import RpcClient

from common.amqp_worker import AMQPWorker

# Redis library
from redis import Redis

# HTTP client and server (for optional health checks or external calls)
from aiohttp import web, ClientSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize RPC client
rpc_client = RpcClient()

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

worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="order_queue",
)