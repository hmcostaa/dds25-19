import os
import json
import time
import logging
import hashlib
import redis
from msgspec import msgpack, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError

from typing import Optional, Tuple, Dict, Any

# Configure logging
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
idempotency_db = redis.Redis(
    host=IDEMPOTENCY_REDIS_HOST,
    port=IDEMPOTENCY_REDIS_PORT,
    password=IDEMPOTENCY_REDIS_PASSWORD,
    db=IDEMPOTENCY_REDIS_DB,
    decode_responses=True
)

try:
    idempotency_db.ping()
    logger.info(f"Connected to idempotency Redis at {IDEMPOTENCY_REDIS_HOST}:{IDEMPOTENCY_REDIS_PORT}")
except redis.ConnectionError as e:
    logger.error(f"Failed to connect to idempotency Redis: {str(e)}")
    # Don't raise exception here - just log it, so service can still start
    # and attempt to reconnect later

#? moved to each service

# def generate_idempotency_key(service_name:str,operation_type:str,resource_id: str,request_id:str=None)->str:
#     """
#         Generate a globally unique idempotent key that includes service information.

#         Parameters:
#         - service_name: Name of the service (e.g., 'order', 'payment', 'stock')
#         - operation_type: Type of operation (e.g., 'create', 'update', 'reservation')
#         - resource_id: ID of the resource being operated on (e.g., order_id)
#         - request_id: Optional client-provided request ID

#         Returns:
#         - A string key in the format 'idempotent:{service}:{operation}:{resource}:{request_id or hash}'
#         """
#     if request_id:
#         return f"idempotent:{service_name}:{operation_type}:{resource_id}:{request_id}"
#     else:

#         # hash_content = f"{service_name}:{operation_type}:{resource_id}:{time.time()}"
#         hash_value=hashlib.sha256(hash_content.encode()).hexdigest()
#         return f"idempotent:{service_name}:{operation_type}:{resource_id}:{hash_value}"

#need status code, solely a dict loses the status code
IdempotencyResultTuple = Tuple[Dict[str, Any], int]

    
def check_idempotent_operation(key:str, redis_conn: redis.Redis)->Optional[IdempotencyResultTuple]:
#retry implementation?
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
        key:str,
        result_tuple:IdempotencyResultTuple, #didnt match check idempotent
        redis_conn: redis.Redis,
        metadata:Dict=None) -> bool:
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
        result_bytes = msgpack.encode(result_tuple)
        
    except MsgspecDecodeError as e:
        logger.error(f"Failed to encode msgpack {key}: {e}")
        raise IdempotencyDataError(f"Failed to encode msgpack {key}: {e}")
    
    try:
        # idempotency_db.set(key, result_bytes, ex=IDEMPOTENT_KEY_TTL, nx=True)
        # line executed regardless whether set succeeded or not --> always true was returned
        # set should have been 'captured'
        was_set = redis_conn.set(key, result_bytes, ex=IDEMPOTENT_KEY_TTL, nx=True)
        return was_set is True

    except redis.exceptions.RedisError as e:
        logger.error(f"Redis error storing idempotency result: {str(e)}")

    except Exception as e:
        logger.error(f"Error storing idempotency result: {str(e)}")
        return False
    
# def clear_idempotent_key_for_order(resource_id: str)->int:
#     """
#         Clear all idempotency keys associated with a specific resource.
#         Useful for testing or manual intervention.

#         Parameters:
#         - resource_id: The resource ID (e.g., order_id)

#         Returns:
#         - Number of keys removed
#         """
#     try:
#         resource_key=f"resource_keys:{resource_id}"
#         keys=idempotency_db.smembers(resource_key)
#         count=0

#     #pipe?
#         for key in keys:
#             idempotency_db.delete(key)
#             count+=1
#         idempotency_db.delete(resource_key)
#         return count
#     except Exception as e:
#         logger.error(f"Error clearing idempotency keys for resource {resource_id}: {str(e)}")
#         return 0


    # potentially abstract idempotent operations?
# def execute_idempotent_operation(key:str,operation_function, *args, **kwargs):
    
