import logging
import os
import atexit
import sys
import sys
import uuid
import asyncio
import redis.asyncio as redis
from redis.asyncio.sentinel import Sentinel 
from flask import Flask, jsonify, abort, Response
from typing import Optional, Tuple, Any, Union, Tuple, Dict
from redis.exceptions import WatchError, RedisError
import random
import logging

from global_idempotency.idempotency_decorator import idempotent

from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("payment-service")
SERVICE_NAME = "payment"

IdempotencyResultTuple = Tuple[Dict[str, Any], int]

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from common.amqp_worker import AMQPWorker


worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)

DB_ERROR_STR = "DB error"

sentinel_async = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26379),
    (os.environ['REDIS_SENTINEL_3'], 26379)],
    socket_timeout=10, #TODO check if this is the right value, potentially lower it
    socket_connect_timeout=5,
    socket_keepalive=True,
    password=os.environ['REDIS_PASSWORD'],
    decode_responses=False
)

db_master = sentinel_async.master_for('payment-master', decode_responses=False)
db_slave = sentinel_async.slave_for('payment-master', decode_responses=False)
logging.info("Connected to Redis Sentinel.")

#changed.. now saga master is used for idempotency as centralized client
# Read connection details from environment variables

idempotency_redis_db = int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0)) 
idempotency_redis_client = sentinel_async.master_for(
    "saga-master",
    decode_responses=False,
    db=idempotency_redis_db
)

logging.info("Connected to idempotency (Payment) Redis.")


async def close_db_connection():
    await db_master.close()
    await db_slave.close()
    await idempotency_redis_client.close()



atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int

async def get_user_from_db(user_id: str) -> Tuple[Union[UserValue, Dict], int]: 
    print(f"--- PAYMENT: get_user_from_db: ENTERED for user_id={user_id}")
    try:
        entry: Optional[bytes] = await db_slave.get(user_id)
    except redis.exceptions.RedisError as redis_err:
        logging.error(f"Redis GET ERROR: {redis_err}")
        return {"error": DB_ERROR_STR}, 500
    if entry is None:
        logging.error(f"User {user_id} NOT FOUND")
        return {"error": f"User: {user_id} not found!"}, 404
    try:
        user_entry: UserValue = msgpack.decode(entry, type=UserValue)
        print(f"--- PAYMENT: get_user_from_db: Decode SUCCESS")
        return user_entry, 200
    except (MsgspecDecodeError, TypeError) as decode_err:
         print(f"--- PAYMENT: get_user_from_db: Decode FAILED: {decode_err}")
         return {"error": "Internal data format error"}, 500

async def atomic_update_user(user_id: str, update_func):
    #retry? backoff/?
    #for attempts i tn range(max_retries): 
    max_retries = 5
    base_backoff = 50 

    # async with db.pipeline(transaction=True) as pipe: # async pipeline, should probably be used TODO
    for attempt in range(max_retries):
        # pipe = db_master.pipeline(transaction=True)
        try:
            async with db_master.pipeline(transaction=True) as pipe:
                #idempotency check?
                # if ?
                # max retry attempts
                #watche exec pipeline

                # await pipe.watch(user_id)
                await pipe.watch(user_id)

                #bytes/?
                entry = await pipe.get(user_id) #noawait

                # entry = await pipe.get(user_id)
                if entry is None:
                        await pipe.unwatch()# quick reset before returning
                        logging.warning("Atomic update: User: %s", user_id)
                        return None, f"User {user_id} not found"
                
                #disistuingish between leader and follower??
                #db_master.pipeline()?
                try:
                    user_val: UserValue = msgpack.Decoder(UserValue).decode(entry)
                    await pipe.unwatch()
                except MsgspecDecodeError:
                    await pipe.unwatch()
                    logging.error("Atomic update: Decode error for user %s", user_id)
                    return None, "Internal data format error"
                
                try:
                    updated_user = update_func(user_val)
                    #synchronous
                    #error?
                except ValueError as e:
                    await pipe.unwatch()
                    raise e
                except Exception as e:
                    await pipe.unwatch()
                    return None, f"Unexpected error in atomic_update_user for user {user_id}: {e}"
            
                pipe.multi()
                pipe.set(user_id, msgpack.Encoder().encode(updated_user))

                results = await pipe.execute() #execute if no other client modified user_id (pipe.watch)
                return updated_user, None #succesfull
                
                
        except WatchError:
            #key was modified between watch and execute, retry backoff 
            #backoff?? TODO
            logging.warning(f"WatchError: key was modified between watch and execute, retry backoff")
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            
            await asyncio.sleep(backoff / 1000)
            continue #loop again

        except redis.RedisError:
            return None, DB_ERROR_STR
        
        except Exception as e:
            logging.exception(f"Unexpected error in atomic_update_user for user {user_id}: {e}")
            return None, "Internal data error"
            
    return None, f"Failed to update user, retries nr??."

# For testing, had endpoint "user_id" couldnt be found using POSTMAN
def require_user_id(data):
    uid = data.get("user_id")
    if not uid:
        return None, ({"error": "Missing 'user_id' in payload"}, 400)
    return str(uid).strip()

@worker.register
@idempotent('create_user', idempotency_redis_client, SERVICE_NAME) #not sure if idempotent or not TODO
async def create_user(data, message):
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        await db_master.set(key, value)
        return {'user_id': key}, 200
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    except Exception as e:
        logging.exception("Error creating user: %s", e)
        return {"error": f"Error creating user: {e}"}, 400
    
     
@worker.register
async def batch_init_users(data, message):
    n = int(data['n'])
    starting_money = int(data['starting_money'])
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        await db_master.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for users successful"}, 200


@worker.register
async def find_user(data, message):
    try:
        user_id = data.get("user_id")
        user_entry, status_code = await get_user_from_db(user_id)
        if status_code != 200:
            return {"error": f"Error retrieving user from DB for id {user_id}: {status_code}"}, status_code
        
        user_entry2 = user_entry # to avoid status code error
        response = {"user_id": user_id, "credit": user_entry2.credit}
        logging.debug(f"Found user {user_id} with credit {user_entry2.credit}")
        return response, 200
    except Exception as e:
        logging.exception("Error retrieving user from DB for id %s: %s", user_id, e)
        return {"error": f"Error retrieving user from DB for id {user_id}: {e}"}, 500
    
@worker.register
@idempotent('add_funds', idempotency_redis_client, SERVICE_NAME)
async def add_funds(data, message) -> IdempotencyResultTuple:
    user_id =  require_user_id(data)
    amount = int(data.get("amount", 0))
    
    if amount <= 0:
        return {"error": "Transaction amount must be positive"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user
    
    
    # decorator now handles try except
    # atomic_update_user(data["user_id"], updater)
    updated_user, error_msg = await atomic_update_user(user_id, updater)

    if error_msg:
        logging.error(f"Failed to add funds for user {user_id}: {error_msg}")
        return {"error": f"Failed to add funds: {error_msg}"}, 500 

    if updated_user:
        return {"done": True, "credit": updated_user.credit}, 200
    else:
        return {"error": "Failed to update user, unknown reason"}, 500
    
@worker.register
@idempotent('pay', idempotency_redis_client, SERVICE_NAME)
async def pay(data, message) -> IdempotencyResultTuple:
    user_id = data.get("user_id")
    amount = data.get("amount")

    if not all([user_id, amount is not None]):
        logging.error("Missing required fields (user_id amount)")
        return {"error": "Missing required fields"}, 400
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logging.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400

    def updater(user: UserValue) -> UserValue:
        if user.credit < int(amount):
            raise ValueError(f"User {user_id} has insufficient credit")
        user.credit -= int(amount)
        return user

    updated_user = None
    error_msg = None
    response_tuple = None 

    try:
        logging.info(f"--- PAYMENT PAY HANDLER: Calling atomic_update_user for user {user_id}")
        updated_user, error_msg = await atomic_update_user(user_id, updater)

        if updated_user:
            response_tuple = ({"paid": True, "credit": updated_user.credit}, 200)
        elif error_msg:
            status_code = 400 if "insufficient credit" in error_msg.lower() else (404 if "not found" in error_msg.lower() else 500)
            response_tuple = ({"paid": False, "error": error_msg}, status_code)
        else:
            response_tuple = ({"paid": False, "error": "Internal processing error"}, 500)

        logging.info(f"--- PAYMENT PAY HANDLER: Returning response tuple: {response_tuple}")
        return response_tuple 

    except ValueError as e:
        response_tuple = ({"paid": False, "error": str(e)}, 400 if "insufficient credit" in str(e).lower() else 400)
        logging.error(f"--- PAYMENT PAY HANDLER: Caught ValueError: {e}, returning: {response_tuple}")
        return response_tuple
    except Exception as e:
        response_tuple = ({"paid": False, "internal error": str(e)}, 500) 
        logging.exception(f"--- PAYMENT PAY HANDLER: Caught Exception: {e}, returning: {response_tuple}")
        return response_tuple 

@worker.register
@idempotent('remove_credit', idempotency_redis_client, SERVICE_NAME)
async def remove_credit(data, message)-> IdempotencyResultTuple:
    user_id = data.get("user_id")
    amount = data.get("amount")

    if not user_id or amount <= 0:
        return {"error": "Invalid request"}, 400

    def updater(user: UserValue) -> UserValue:
        if user.credit < amount:
            raise ValueError("Insufficient funds")
        user.credit -= amount
        return user

    updated_user, error_msg = await atomic_update_user(user_id, updater)

    if error_msg:
        return {"error": error_msg}, 400
    return {"msg": f"Removed {amount} from user {user_id}"}, 200

@worker.register
@idempotent('compensate', idempotency_redis_client, SERVICE_NAME)
async def compensate(data, message):
    try:
        user_id = data.get("user_id")
        amount = data.get("amount")

        if not all([user_id, amount is not None]):
            logging.error("Missing user_id or amount in cancel_payment request.")
            return {"error": "Missing required fields"}, 400
        
        if int(amount) <= 0:
            return {"error": "Transaction amount must be positive"}, 400
        
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    
    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user
    
    try:
        updated_user, error_msg = await atomic_update_user(user_id, updater)
        if error_msg:
            logging.error(f"Failed to cancel payment (refund) for user {user_id}: {error_msg}")
            status_code = 404 if "not found" in error_msg.lower() else 500
            return {"error": f"Failed to cancel payment: {error_msg}"}, status_code
        if updated_user:
            return {"refunded": True, "credit": updated_user.credit}, 200
        else:
            logging.error(f"Cancel payment failed for user {user_id}, unknown reason .")
            return {"error": "Failed cancellation, unknown reason"}, 500
    except Exception as e:
        logging.exception("Error canceling payment for user %s: %s", user_id, e)
        return {"error": f"Error canceling payment for user {user_id}: {e}"}, 400
    
if __name__ == '__main__':
    logging.info("Starting payment service AMQP worker...")
    asyncio.run(worker.start())
