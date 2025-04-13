import functools
import logging
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel 
from msgspec import msgpack, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from typing import Tuple, Any, Dict
import hashlib
import json
import logging

logger = logging.getLogger("idempotency_deco")

IDEMPOTENT_KEY_TTL = 86400 * 7

SERVICE_CONTEXT = {
    "payment": "user_id",
    "stock": "item_id",
    "order": "order_id",
}

#helps debugging later on
class IdempotencyStoreConnectionError(Exception):
    """exception for Redis connection or command errors."""
    pass

class IdempotencyDataError(Exception):
    """exception for data format/serialization errors."""
    pass

class IdempotencyStoreConnectionError(Exception):
    """exception for when the Redis connection fails."""
    pass

IdempotencyResultTuple = Tuple[Dict[str, Any], int]

def create_hash(payload: dict, exclude_keys: set = None) -> str:
    if exclude_keys is None:
        exclude_keys = {'attempt_id', 'order_id', 'user_id', 'item_id'}
    
    #exclude metadata since two duplicates that should perform the same operation can have different metadata
    relevant = {keys: values for keys, values in payload.items() if keys not in exclude_keys and values is not None}
    if not relevant:
        return "backup_payload_hash_not_relevant"
    
    return hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()
def idempotent(operation_name: str, redis_client: redis.Redis, service_name: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(data: dict, *args, **kwargs):
            #improved idempotency key to not rely on some of the values since they are not consistent for each service
            unique_key: str | None = data.get("idempotency_key")
            
            if not unique_key:
                #fallback, shouldnt occur
                logger.warning(f"No idempotency key found in service name {service_name} or operation name {operation_name}")
                idempotency_key_falback = data.get("idempotency_key")
                if idempotency_key_falback:
                    unique_key = idempotency_key_falback
                else:
                    #last resort
                    order_id = data.get("order_id")
                    attempt_id = data.get("attempt_id")
                    if order_id and attempt_id:
                        unique_key = f"saga:{order_id}:{attempt_id}"
                        logger.debug(f"Falling back to last resot saga unique key: {unique_key}")

            if not unique_key:
                #if even that fails, return error
                logger.error(f"No idempotency key found in service name {service_name} or operation name {operation_name}")
                return {"error": "Idempotency error - no key found"}, 500
            
            context_key = SERVICE_CONTEXT.get(service_name)
            context_id =data.get(context_key) if context_key else "no_context" 
            if not context_key and context_id:
                context_id = "no_context"

            created_hash = create_hash(data, exclude_keys={"idempotency_key", "order_id", "attempt_id", context_key})

            redis_key = f"idempotency:{service_name}:{operation_name}:{context_id}:{unique_key}:{created_hash}"
            logger.debug(f"Idempotency Check - Key: {redis_key}")

            #maybe cache??
            #store cache here potentially TODO
            #cache miss
            try:
                cache_bytes = await redis_client.get(redis_key)
                if cache_bytes:
                    logger.info(f"Cache hit for key {redis_key}")
                    try:
                        result_tuple = msgpack.decode(cache_bytes)
                        return result_tuple
                    except MsgspecDecodeError as e:
                        logger.error(f"Error decoding cache_bytes for key {redis_key}: {e}")
                        return {"error": "Idempotency internal error"}, 500
                else:
                    logger.debug(f"Cache miss for key {redis_key}")
                
                    result_tuple: IdempotencyResultTuple | None = None
                    try:
                        logger.debug(f"Original function: {func.__name__}")
                        result_tuple = await func(data, *args, **kwargs)

                        if not (isinstance(result_tuple, tuple) and len(result_tuple) == 2 and isinstance(result_tuple[1], int)):
                            logger.error(f"Decorated function '{func.__name__}' returned invalid result_tuple : {type(result_tuple)}. Expected (dict, int).")
                            raise 
                        
                        logger.debug(f"Decorated function '{func.__name__}' returned result tuple: {result_tuple}")
                    except Exception as e:
                        logger.error(f"Error in idempotency decorator: {str(e)}")
                        status_code = 500
                        if isinstance(e, ValueError):
                            status_code = 400
                        
                        result_tuple = ({"error": str(e)}, status_code)
                
                    try:
                        bytes = msgpack.encode(result_tuple)
                        bytes_were_set = await redis_client.set(redis_key, bytes, nx=True, ex=IDEMPOTENT_KEY_TTL)
                        if bytes_were_set:
                            logger.info(f"Stored idempotency result for key {redis_key}")
                        else:
                            #race conition, retry
                            await asyncio.sleep(0.05)
                            retry_bytes = await redis_client.get(redis_key)
                            if retry_bytes:
                                try:
                                    result_tuple = msgpack.decode(retry_bytes)
                                    logger.info(f"Successfully retrieved idempotency result for key {redis_key}")
                                except (MsgspecDecodeError, TypeError) as e:
                                    logger.error(f"Error decoding idempotency result for key {redis_key}: {e}")
                                    logger.warning("Falling back to original redis key: {redis_key}")
                            else:
                                logger.debug(f"refetch failed for key {redis_key}")
                    except (MsgspecEncodeError, TypeError, ValueError) as e:
                        logger.error(f"error encoding result tuple for key {redis_key}: {e}")
                        return {"error": "Idempotency error"}, 500
                    except redis.exceptions.RedisError as e:
                        logger.erro(f"Redis error storing result tuple for key {redis_key}: {e}")
                        return {"error": "Idempotency store error"}, 503
                    
                    return result_tuple
            
            except redis.exceptions.RedisError as e:
                logger.error(f"Redis error getting cache_bytes for key {redis_key}: {e}")
                return {"error": "Idempotency error"}, 503
            except Exception as e:
                logger.error(f"Unexpected error  {redis_key}: {e}")
        return wrapper
    return decorator
