import functools
import logging
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel 
from msgspec import msgpack, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from typing import Tuple, Any, Dict
import hashlib
import json
import logging
import time
import asyncio

# Configure detailed logging
logger = logging.getLogger("idempotency_deco")
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

IDEMPOTENT_KEY_TTL = 86400 * 7  

SERVICE_CONTEXT = {
    "payment": "user_id",
    "stock": "item_id",
    "order": "order_id",
}
logger.debug(f"[IDEMPOTENCY] Configured service context mappings: {SERVICE_CONTEXT}")

#helps debugging later on
class IdempotencyStoreConnectionError(Exception):
    def __init__(self, message="Redis connection error in idempotency store"):
        logger.error(f"[IDEMPOTENCY] {message}")
        super().__init__(message)

class IdempotencyDataError(Exception):
    def __init__(self, message="Data format error in idempotency store"):
        logger.error(f"[IDEMPOTENCY] {message}")
        super().__init__(message)

class IdempotencyStoreConnectionError(Exception):
    """exception for when the Redis connection fails."""
    pass

IdempotencyResultTuple = Tuple[Dict[str, Any], int]

def create_hash(payload: dict, exclude_keys: set = None) -> str:
    logger.debug(f"[IDEMPOTENCY:HASH] Creating hash with payload size: {len(json.dumps(payload))} bytes")
    
    if exclude_keys is None:
        exclude_keys = {'attempt_id', 'order_id', 'user_id', 'item_id'}
        logger.debug(f"[IDEMPOTENCY:HASH] Using default exclude keys: {exclude_keys}")
    
    #exclude metadata since two duplicates that should perform the same operation can have different metadata
    relevant = {keys: values for keys, values in payload.items() if keys not in exclude_keys and values is not None}
    
    if not relevant:
        logger.warning(f"[IDEMPOTENCY:HASH] No relevant payload data after excluding keys")
        return "backup_payload_hash_not_relevant"
    
    logger.debug(f"[IDEMPOTENCY:HASH] Relevant payload keys: {list(relevant.keys())}")
    
    hash_value = hashlib.sha256(json.dumps(relevant, sort_keys=True).encode()).hexdigest()
    
    return hash_value

def idempotent(operation_name: str, redis_client: redis.Redis, service_name: str):
    logger.info(f"[IDEMPOTENCY] Setting up idempotent decorator for service={service_name}, operation={operation_name}")
    
    def decorator(func):
        
        @functools.wraps(func)
        async def wrapper(data: dict, *args, **kwargs):
            func_name = func.__name__
            
            logger.debug(f"[IDEMPOTENCY:{func_name}] Input data keys: {list(data.keys())}")
            
            #improved idempotency key to not rely on some of the values since they are not consistent for each service
            unique_key: str | None = data.get("idempotency_key")
            
            if not unique_key:
                #fallback, shouldnt occur
                logger.warning(f"[IDEMPOTENCY:{func_name}] No idempotency key found in service name {service_name} or operation name {operation_name}")
                idempotency_key_falback = data.get("idempotency_key")
                
                if idempotency_key_falback:
                    unique_key = idempotency_key_falback
                    logger.debug(f"[IDEMPOTENCY:{func_name}] Using fallback idempotency key: {unique_key}")
                else:
                    #last resort
                    order_id = data.get("order_id")
                    attempt_id = data.get("attempt_id")
                    
                    if order_id and attempt_id:
                        unique_key = f"saga:{order_id}:{attempt_id}"
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Falling back to last resort saga unique key: {unique_key}")

            if not unique_key:
                #if even that fails, return error
                error_msg = f"No idempotency key found in service name {service_name} or operation name {operation_name}"
                logger.error(f"No idempotency key found in service name {service_name} or operation name {operation_name}")  
                
                return {"error": "Idempotency error - no key found"}, 500
            
            # Get context key and ID
            context_key = SERVICE_CONTEXT.get(service_name)
            logger.debug(f"[IDEMPOTENCY:{func_name}] Context key for service {service_name}: {context_key}")
            
            context_id = data.get(context_key) if context_key else "no_context" 
            if not context_key and context_id:
                context_id = "no_context"
                
            logger.debug(f"[IDEMPOTENCY:{func_name}] Context ID: {context_id}")

            logger.debug(f"[IDEMPOTENCY:{func_name}] Creating hash for payload")
            created_hash = create_hash(data, exclude_keys={"idempotency_key", "order_id", "attempt_id", context_key})
            logger.debug(f"[IDEMPOTENCY:{func_name}] Created hash: {created_hash}")

            redis_key = f"idempotency:{service_name}:{operation_name}:{context_id}:{unique_key}:{created_hash}"
            logger.debug(f"[IDEMPOTENCY:{func_name}] Redis key: {redis_key}")
            logger.debug(f"Idempotency Check - Key: {redis_key}")  

            #maybe cache??
            #store cache here potentially TODO
            #cache miss
            try:
                cache_bytes = await redis_client.get(redis_key)
                
                if cache_bytes:
                    cache_size = len(cache_bytes)
                    logger.info(f"[IDEMPOTENCY:{func_name}] Cache hit for key {redis_key}, size={cache_size} bytes")
                    
                    try: 
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Decoding cached result")
                        decoded_result = msgpack.decode(cache_bytes)
                        
                        result_tuple = None 

                        if isinstance(decoded_result, list) and len(decoded_result) == 2:
                            result_tuple = tuple(decoded_result)
                            logger.debug(f"[IDEMPOTENCY:{func_name}] Converted decoded list to tuple for cache hit: {redis_key}")
                        elif isinstance(decoded_result, tuple) and len(decoded_result) == 2:
                            result_tuple = decoded_result 
                            logger.debug(f"[IDEMPOTENCY:{func_name}] Decoded result already in tuple format")

                        if not (result_tuple and
                                isinstance(result_tuple[0], dict) and
                                isinstance(result_tuple[1], int)):
                            if result_tuple: 
                                logger.error(f"[IDEMPOTENCY:{func_name}] Cached data has invalid element types: ({type(result_tuple[0])}, {type(result_tuple[1])})")
                                logger.error(f"Cached data structure has invalid element types for key {redis_key}: ({type(result_tuple[0])}, {type(result_tuple[1])})")  
                                raise IdempotencyDataError(f"Invalid element types in cached data for key {redis_key}")
                            else: 
                                logger.error(f"Unexpected data structure decoded from cache for key {redis_key}: {type(decoded_result)}") 
                                raise IdempotencyDataError(f"Unexpected data structure in cache for key {redis_key}")

                        logger.debug(f"Returning successfully processed cached result tuple for key {redis_key}")  
                        
                        return result_tuple
                        
                    except MsgspecDecodeError as e:
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Cache bytes size: {len(cache_bytes)}, first 50 bytes: {cache_bytes[:50]}")
                        return {"error": "Idempotency internal error"}, 500
                else:
                    logger.debug(f"[IDEMPOTENCY:{func_name}] Cache miss for key {redis_key}")
                    logger.debug(f"Cache miss for key {redis_key}")  
                
                    result_tuple: IdempotencyResultTuple | None = None
                    try:
                        logger.debug(f"Original function: {func.__name__}")
                        logger.info(f"[IDEMPOTENCY:{func_name}] Cache miss, executing original function: {func.__name__}")
                        
                        result_tuple = await func(data, *args, **kwargs)

                        if not (isinstance(result_tuple, tuple) and len(result_tuple) == 2 and isinstance(result_tuple[1], int)):
                            error_msg = f"Function '{func.__name__}' returned invalid result: {type(result_tuple)}. Expected (dict, int)."
                            logger.error(f"[IDEMPOTENCY:{func_name}] {error_msg}")
                            raise TypeError(error_msg)
                        
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Function result status code: {result_tuple[1]}")
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Function result data keys: {list(result_tuple[0].keys()) if isinstance(result_tuple[0], dict) else 'not a dict'}")
                        logger.debug(f"Decorated function '{func.__name__}' returned result tuple: {result_tuple}")  
                        
                    except Exception as e:
                        logger.error(f"[IDEMPOTENCY:{func_name}] Error executing function: {str(e)}", exc_info=True)
                        
                        status_code = 500
                        if isinstance(e, ValueError):
                            status_code = 400
                            logger.debug(f"[IDEMPOTENCY:{func_name}] ValueError detected, using status code 400")
                        
                        result_tuple = ({"error": str(e)}, status_code)
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Created error result: {result_tuple}")
                
                    try:
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Encoding result tuple for storage")
                        bytes_data = msgpack.encode(result_tuple)
                        
                        logger.debug(f"[IDEMPOTENCY:{func_name}] Storing result in Redis with TTL={IDEMPOTENT_KEY_TTL}s")
                        bytes_were_set = await redis_client.set(redis_key, bytes_data, nx=True, ex=IDEMPOTENT_KEY_TTL)
                        
                        if bytes_were_set:
                            logger.info(f"[IDEMPOTENCY:{func_name}] Successfully stored result in Redis for key: {redis_key}")
                            logger.info(f"Stored idempotency result for key {redis_key}")  
                        else:
                            await asyncio.sleep(0.05)
                            
                            retry_bytes = await redis_client.get(redis_key)
                            if retry_bytes:
                                try:
                                    result_tuple = msgpack.decode(retry_bytes)
                                    
                                except (MsgspecDecodeError, TypeError) as e:
                                    logger.error(f"[IDEMPOTENCY:{func_name}] Error decoding retry result: {str(e)}")
                                    logger.warning("Falling back to original redis key: {redis_key}")  
                            else:
                                logger.warning(f"[IDEMPOTENCY:{func_name}] Retry GET failed, key not found")
                                logger.debug(f"refetch failed for key {redis_key}")  
                    except (MsgspecEncodeError, TypeError, ValueError) as e:
                        logger.error(f"error encoding result tuple for key {redis_key}: {e}")
                        return {"error": "Idempotency error"}, 500
                    except redis.exceptions.RedisError as e:
                        logger.error(f"Redis error storing result tuple for key {redis_key}: {e}")  
                        return {"error": "Idempotency store error"}, 503
                    
                    return result_tuple
            
            except redis.exceptions.RedisError as e:
                logger.error(f"[IDEMPOTENCY:{func_name}] Redis error accessing cache: {str(e)}", exc_info=True)
                return {"error": "Idempotency error"}, 503
            except Exception as e:
                logger.error(f"[IDEMPOTENCY:{func_name}] Unexpected error: {str(e)}", exc_info=True)
                return {"error": "Idempotency internal error"}, 500
                
        logger.debug(f"[IDEMPOTENCY] Successfully created wrapper for {func.__name__}")
        return wrapper
        
    return decorator
