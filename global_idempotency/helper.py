


import os
import json
import time
import logging
import hashlib
import redis
from msgspec import msgpack, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError

from typing import Optional, Tuple, Dict, Any


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("idempotency-service")

# Constants
IDEMPOTENT_KEY_TTL = 86400 * 7


#helps debugging later on
class IdempotencyStoreConnectionError(Exception):
    """exception for Redis connection or command errors."""
    pass

class IdempotencyDataError(Exception):
    """exception for data format/serialization errors."""
    pass

IDEMPOTENCY_REDIS_HOST = os.environ.get('IDEMPOTENCY_REDIS_HOST', 'redis-idempotency')
IDEMPOTENCY_REDIS_PORT = int(os.environ.get('IDEMPOTENCY_REDIS_PORT', 6379))
IDEMPOTENCY_REDIS_PASSWORD = os.environ.get('IDEMPOTENCY_REDIS_PASSWORD', None)
IDEMPOTENCY_REDIS_DB = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0))

# Create a dedicated Redis client for idempotency
idempotency_db_conn = redis.Redis(
    host=os.environ.get('IDEMPOTENCY_REDIS_HOST', os.environ.get('REDIS_HOST', 'localhost')),
    port=int(os.environ.get('IDEMPOTENCY_REDIS_PORT', os.environ.get('REDIS_PORT', 6379))),
    password=os.environ.get('IDEMPOTENCY_REDIS_PASSWORD', os.environ.get('REDIS_PASSWORD', None)),
    db=int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0)),
    decode_responses=False
)
IdempotencyResultTuple = Tuple[Dict[str, Any], int]

try:
    idempotency_db_conn.ping()
    logger.info(f"Connected to idempotency Redis at {IDEMPOTENCY_REDIS_HOST}:{IDEMPOTENCY_REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to idempotency Redis: {str(e)}")

def generate_idempotency_key(SERVICE_NAME, user_id, order_id, attempt_id):
    idempotency_key = f"idempotent:{SERVICE_NAME}:pay:{user_id}:{order_id}:{attempt_id}"
    idempotency_utf8 = idempotency_key.encode('utf-8')
    return idempotency_key,idempotency_utf8


def check_idempotent_operation(key: str, redis_conn: redis.Redis) -> Optional[IdempotencyResultTuple]:
    # retry implementation?
    try:
        result_bytes = redis_conn.get(key)
        if result_bytes is None:
            return None
        try:
            # similar to framework implemntation for stock/payment, instead of json.loads
            result: IdempotencyResultTuple = msgpack.decode(
                result_bytes,
                type=IdempotencyResultTuple
            )

            if result:
                return result
            else:
                logger.error(f"Error type {type(result)}")
                raise IdempotencyDataError(f"Invalid data structure found for key {key}")
        except MsgspecDecodeError as e:
            logger.error(f"Failed to decode msgpack {key}: {e}")
            raise IdempotencyDataError(f"Failed to decode msgpack {key}: {e}")

    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error checking idempotency key {key}: {e}")
        raise IdempotencyStoreConnectionError(f"Redis error checking key {key}: {e}")

    except Exception as e:
        logger.error(f"Error checking idempotency key: {str(e)}")
        raise IdempotencyStoreConnectionError(f"Unexpected error checking key {key}: {e}")


def store_idempotent_result(
        key: str,
        result: Dict,
        metadata: Dict = None) -> bool:
    """
        Store the result of an operation for future idempotency checks.

        Parameters:
        - key: The idempotency key
        - result: The result to store
        - metadata: Optional metadata about the operation (timing, version, etc.)

        Returns:
        - True if storage was successful, False otherwise
        """
    try:
        result_bytes = msgpack.encode(result)

    except MsgspecDecodeError as e:
        logger.error(f"Failed to encode msgpack {key}: {e}")
        raise IdempotencyDataError(f"Failed to encode msgpack {key}: {e}")

    try:
        idempotency_db_conn.set(key, result_bytes, ex=IDEMPOTENT_KEY_TTL, nx=True)
        return True
    except Exception as e:
        logger.error(f"Error storing idempotency result: {str(e)}")
        return False

def check_idempotency_key(key: str)->bool:
    redis_conn = idempotency_db_conn
    try:
        return redis_conn.exists(key) == 1
    except Exception as e:
        print(f"[IDEMPOTENCY] Redis error while checking key {key}: {e}")
        return False
