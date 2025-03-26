import os
import json
import time
import logging
import hashlib
import redis
from typing import Dict, Optional, Any, Union

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("idempotency-service")

# Constants
IDEMPOTENT_KEY_TTL = 86400 * 7

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

def generate_idempotency_key(service_name:str,operation_type:str,resource_id: str,request_id:str=None)->str:
    """
        Generate a globally unique idempotent key that includes service information.

        Parameters:
        - service_name: Name of the service (e.g., 'order', 'payment', 'stock')
        - operation_type: Type of operation (e.g., 'create', 'update', 'reservation')
        - resource_id: ID of the resource being operated on (e.g., order_id)
        - request_id: Optional client-provided request ID

        Returns:
        - A string key in the format 'idempotent:{service}:{operation}:{resource}:{request_id or hash}'
        """
    if request_id:
        return f"idempotent:{service_name}:{operation_type}:{resource_id}:{request_id}"
    else:
        hash_content = f"{service_name}:{operation_type}:{resource_id}:{time.time()}"
        hash_value=hashlib.sha256(hash_content.encode()).hexdigest()
        return f"idempotent:{service_name}:{operation_type}:{resource_id}:{hash_value}"
def check_idempotent_operation(key:str)->Optional[Dict]:
    try:
        result_json=idempotency_db.get(key)
        if result_json:
            return json.loads(result_json)
        return None
    except Exception as e:
        logger.error(f"Error checking idempotency key: {str(e)}")
        return None
def store_idempotent_result(key:str,result:Dict,metadata:Dict=None):
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
        if metadata:
            storage_data={
                "result":result,
                "metadata":{
                    **metadata,
                    "stored_at":time.time()
                }
            }
        else:
            storage_data={
                "result":result,
                "metadata":{
                    "stored_at":time.time()
                }
            }
        idempotency_db.set(key,json.dumps(storage_data),ex=IDEMPOTENT_KEY_TTL)
        if ":" in key:
         parts=key.split(":")
         if len(parts)>=4:
            resource_id=parts[3]
            idempotency_db.sadd(f"resource_id:{resource_id}",key)
            idempotency_db.expire(f"resource_id:{resource_id}",IDEMPOTENT_KEY_TTL)
         return True
    except Exception as e:
        logger.error(f"Error storing idempotency result: {str(e)}")
        return False

def clear_idempotent_key_for_order(resource_id: str)->int:
    """
        Clear all idempotency keys associated with a specific resource.
        Useful for testing or manual intervention.

        Parameters:
        - resource_id: The resource ID (e.g., order_id)

        Returns:
        - Number of keys removed
        """
    try:
        resource_key=f"resource_keys:{resource_id}"
        keys=idempotency_db.smembers(resource_key)
        count=0
        for key in keys:
            idempotency_db.delete(key)
            count+=1
        idempotency_db.delete(resource_key)
        return count
    except Exception as e:
        logger.error(f"Error clearing idempotency keys for resource {resource_id}: {str(e)}")
        return 0



