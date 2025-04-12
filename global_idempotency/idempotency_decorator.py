import functools
import logging
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel 
from msgspec import msgpack, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from typing import Tuple, Any, Dict

logger = logging.getLogger("idempotency_deco")

IDEMPOTENT_KEY_TTL = 86400 * 7

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

def idempotent(operation_name: str, redis_client: redis.Redis, service_name: str):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(data: dict, *args, **kwargs):
            user_id = data.get("user_id")  # payment
            item_id = data.get("item_id")  # stock
            order_id = data.get("order_id")
            attempt_id = data.get("attempt_id")
            amount = data.get("amount")

            if not order_id or not attempt_id:
                logger.warning(f"'{operation_name}' called without order_id or attempt_id. Skipping idempotency check.")
                return await func(data, *args, **kwargs)

            key_parts = None #now initialized
            skippable = False

            if order_id and attempt_id:
                if service_name == "payment" and user_id:
                    key_parts = [service_name, operation_name, user_id, order_id, attempt_id]
                elif service_name == "stock" and item_id:
                    key_parts = [service_name, operation_name, item_id, order_id, attempt_id]
                else:
                    key_parts = [service_name, operation_name, order_id, attempt_id]

            elif service_name == "payment" and operation_name == "pay" and user_id and amount is not None:
                #payment does not have order_id and attempt_id
                key_parts = [service_name, operation_name, user_id, str(amount)]
            else:
                logger.warning(f"Unknown service name {service_name} or operation name {operation_name}")
                skippable = True
            
            if skippable:
                return await func(data, *args, **kwargs)
                # call the function without idempotency

            redis_key = f"idempotency:{':'.join(str(p) for p in key_parts)}"

            logger.debug(f"Using idempotency key: {redis_key}")

            stored_data = None
            try:
                stored_data = await redis_client.get(redis_key)
                if stored_data:
                    logger.info(f"Idempotency hit for key {redis_key}. Returning stored result.")
                    try:
                        stored_result = msgpack.decode(stored_data, type=IdempotencyResultTuple)
                        return stored_result
                    except IdempotencyDataError as e:
                         logger.error(f"ocrrupted idempotency data for key {redis_key}: {e}")
                        #  return {"error": "internal idempotency data error"}, 500

            except redis.exceptions.ConnectionError as e:
                logger.error(f"redis connection error during idempotency check: {e}")
                # return {"error": "idempotency store unavailable"}, 503
            
            except redis.exceptions.RedisError as e:
                logger.error(f"redis connection error during idempotency check: {e}")
                # return {"error": "idempotency store unavailable/error"}, 503

            try:
                result_tuple: IdempotencyResultTuple= await func(data, *args, **kwargs)
                logger.debug(f"operation for key {redis_key} executed. Result: {result_tuple}")

                if not (isinstance(result_tuple, tuple) and len(result_tuple) == 2 and isinstance(result_tuple[1], int)):
                    logger.warning(f"idempotency result tuple is not a tuple of length 2: {result_tuple}")
                    return result_tuple
                
            except Exception as e:
                logger.error(f"Error in idempotency decorator: {str(e)}")
                return e

            except (redis.exceptions.ConnectionError, redis.exceptions.RedisError) as e:
                 # extra redis error handling
                 logger.critical(f"FAILED to store idempotency result for {redis_key}: {e}")
                 # have to decide --> return the computed result anyway, or an error?
                 # result-> risks non-idempotency on retry if storage failed.
                 # error --> might be safer but could cause NACK/retry loops if the operation *did* succeed.
                 # check again later
                 if result_tuple:
                     return result_tuple
                 else:
                      return {"error": f"Redis error during idempotency storage: {str(e)}"}, 503
            except IdempotencyDataError as e:
                 logger.critical(f"FAILED to serialize idempotency result for {redis_key}: {e}")
                 if result_tuple:
                     return result_tuple # storage failed
                 else:
                      return {"error": f"Serialization error during idempotency storage: {str(e)}"}, 500

            except Exception as e:
                logger.exception(f"Error during execution of wrapped function for key {redis_key}: {e}")
                error_result = ({"error": f"Operation failed: {str(e)}"}, 500) 
                try:
                    serialized_error= msgpack.encode(error_result)
                    if redis_client.set(redis_key, serialized_error, nx=True, ex=IDEMPOTENT_KEY_TTL):
                         logger.info(f"Stored idempotency failure result for key {redis_key}")

                    else:
                         logger.warning(f"Idempotency race condition on failure for key {redis_key}")
                    
                    return error_result 
                except Exception as store_e:
                     logger.critical(f"FAILED to store idempotency failure result for {redis_key}: {store_e}")
                     return error_result
                
            try:
                #now actually storing async 
                result_bytes = msgpack.encode(result_tuple)
                was_set = await redis_client.set(redis_key, result_bytes, nx=True, ex=IDEMPOTENT_KEY_TTL)
                if was_set:
                    logger.info(f"Stored idempotency result for key {redis_key}")
                else:
                    logger.warning(f"Idempotency race condition on success for key {redis_key}")
            except Exception as e:
                logger.error(f"Error storing idempotency result for key {redis_key}: {str(e)}")
        
            return result_tuple

        return wrapper
    return decorator
