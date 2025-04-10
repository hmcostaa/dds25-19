import logging
import os
import atexit
import sys
import uuid
import asyncio

import redis

from flask import Flask, jsonify, abort, Response
from typing import Optional, Tuple

from redis import Sentinel
from redis.exceptions import WatchError, RedisError
import random
import logging
from msgspec import msgpack, Struct, DecodeError as MsgspecDecodeError, EncodeError as MsgspecEncodeError
from global_idempotency.app import (
        IdempotencyStoreConnectionError,
        IdempotencyDataError,
        check_idempotent_operation,
        store_idempotent_result,
        IdempotencyResultTuple
    )
from global_idempotency import helper

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

logging = logging.getLogger("payment-service")
SERVICE_NAME = "payment"


sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from common.amqp_worker import AMQPWorker


worker = AMQPWorker(
    amqp_url=os.environ["AMQP_URL"],
    queue_name="payment_queue",
)

DB_ERROR_STR = "DB error"

#pytest --maxfail=1 --disable-warnings -q


sentinel = Sentinel([
    (os.environ['REDIS_SENTINEL_1'], 26379),
    (os.environ['REDIS_SENTINEL_2'], 26380),
    (os.environ['REDIS_SENTINEL_3'], 26381)], socket_timeout=0.1, password=os.environ['REDIS_PASSWORD'])

db_master = sentinel.master_for('payment-master', socket_timeout=0.1, decode_responses=True)
db_slave = sentinel.slave_for('payment-master', socket_timeout=0.1, decode_responses=True)

db_master.connect()
db_slave.close()
logging.info("Connected to traditional Redis.")

idempotency_db_conn = redis.Redis(
    host=os.environ.get('IDEMPOTENCY_REDIS_HOST', os.environ.get('REDIS_HOST', 'localhost')),
    port=int(os.environ.get('IDEMPOTENCY_REDIS_PORT', os.environ.get('REDIS_PORT', 6379))),
    password=os.environ.get('IDEMPOTENCY_REDIS_PASSWORD', os.environ.get('REDIS_PASSWORD', None)),
    db=int(os.environ.get('IDEMPOTENCY_REDIS_DB', 0)),
    decode_responses=False
)
idempotency_db_conn.ping()
logging.info("Connected to idempotency Redis.")


def close_db_connection():
    db_master.close()
    db_slave.close()


atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> Optional[UserValue]:
    try:
        # get serialized data
        entry: Optional[bytes] = db_slave.get(user_id)
    except redis.exceptions.RedisError:
        return abort(400, DB_ERROR_STR)
    # deserialize data if it exists else return null
    entry: Optional[UserValue] = msgpack.decode(entry, type=UserValue) if entry else None
    if entry is None:
        # if user does not exist in the database; abort
        abort(400, f"User: {user_id} not found!")
    return entry
##CHECK idempotency
##STORE idempotency
# order_id = data.get("order_id") # Used for idempotency key

async def atomic_update_user(user_id: str, update_func):
    #retry? backoff/?
    #for attempts i tn range(max_retries):
    
    max_retries = 5
    base_backoff = 50 

    # async with db.pipeline(transaction=True) as pipe: # async pipeline, should probably be used TODO
    for attempt in range(max_retries):
        pipe = db_master.pipeline(transaction=True)
        try:
            #idempotency check?
            # if ?
            # max retry attempts
            #watche exec pipeline

            # await pipe.watch(user_id)
            pipe.watch(user_id)

            #bytes/?
            entry = pipe.get(user_id) #noawait

            # entry = await pipe.get(user_id)
            if entry is None:
                    pipe.reset() # quick reset before returning
                    logging.warning("Atomic update: User: %s", user_id)
                    return None, f"User {user_id} not found"
            
            #disistuingish between leader and follower??
            #db_master.pipeline()?
            try:
                user_val: UserValue = msgpack.Decoder(UserValue).decode(entry)
            except MsgspecDecodeError:
                pipe.reset()
                logging.error("Atomic update: Decode error for user %s", user_id)
                return None, "Internal data format error"
            
            try:
                updated_user = update_func(user_val)
                #synchronous
                #error?
                pipe.multi()
                pipe.set(user_id, msgpack.Encoder().encode(updated_user))

                pipe.execute() #execute if no other client modified user_id (pipe.watch)
                return updated_user, None #succesfull
            except ValueError as e:
                pipe.reset()
                raise e
                
        except WatchError:
            #key was modified between watch and execute, retry backoff 
            #backoff?? TODO
            backoff_multiplier = (2 ** attempt) * (1 + random.random() * 0.1)
            backoff = base_backoff * backoff_multiplier
            
            await asyncio.sleep(backoff / 1000)
            continue #loop again

        # except redis.WatchError:
        #     continue 
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
async def create_user(data):
    key = str(uuid.uuid4())
    value = msgpack.encode(UserValue(credit=0))
    try:
        db_master.set(key, value)
        return {'user_id': key}, 200
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    except Exception as e:
        logging.exception("Error creating user: %s", e)
        return {"error": f"Error creating user: {e}"}, 400
    
@worker.register
async def batch_init_users(data):
    n = int(data['n'])
    starting_money = int(data['starting_money'])
    kv_pairs: dict[str, bytes] = {f"{i}": msgpack.encode(UserValue(credit=starting_money))
                                  for i in range(n)}
    try:
        db_master.mset(kv_pairs)
    except redis.exceptions.RedisError:
        return {"error": DB_ERROR_STR}, 400
    return {"msg": "Batch init for users successful"}, 200


@worker.register
async def find_user(data):
    try:
        user_id = require_user_id(data)
        user_entry = get_user_from_db(user_id)
        # if error:
        #     return error
        
        response = {"user_id": user_id, "credit": user_entry.credit}
        logging.debug(f"Found user {user_id} with credit {user_entry.credit}")
        return response, 200
    except Exception as e:
        logging.exception("Error retrieving user from DB for id %s: %s", user_id, e)
        abort(400, f"Error retrieving user: {e}")

@worker.register
async def add_funds(data):
    user_id =  require_user_id(data)
    amount = int(data.get("amount", 0))
    
    if amount <= 0:
        return {"error": "Transaction amount must be positive"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user
    
    try:
        # atomic_update_user(data["user_id"], updater)
        updated_user, error_msg = await atomic_update_user(user_id, updater)

        if error_msg:
            logging.error(f"Failed to add funds for user {user_id}: {error_msg}")
            return {"error": f"Failed to add funds: {error_msg}"}, 500 

        if updated_user:
            return {"done": True, "credit": updated_user.credit}, 200
        else:
            return {"error": "Failed to update user, unknown reason"}, 500
    
    except (ValueError, TypeError):
        return {"error": "Invalid amount specified"}, 400
    except Exception as e:
        logging.exception(f"Unexpected error adding funds for user {user_id}: {e}") 
        return {"error": "Unexpected error, add funds"}, 500

@worker.register
async def pay(data):

    # user_id, error = require_user_id(data)
    # if error:
    #     return error
    user_id = data.get("user_id")
    order_id = data.get("order_id")
    amount = int(data.get("amount", 0))
    attempt_id = data.get("attempt_id")

    if not all([user_id, order_id, attempt_id, amount is not None]):
        logging.error("Missing required fields (user_id, order_id, amount, attempt_id)")
        return {"error": "Missing required fields"}, 400
    try:
        amount_int = int(amount)
        if amount_int <= 0:
            return {"error": "Transaction amount must be positive"}, 400
    except (ValueError, TypeError):
        logging.error("Unexpected error, amount:.", amount)
        return {"error": "Invalid amount?"}, 400
    
    idempotency_key,idempotency_utf8 = helper.generate_idempotency_key(SERVICE_NAME, order_id, attempt_id)
    # stored_result = check_idempotency(idempotency_key, idempotency_db_conn)
    # if stored_result is not None:
    #     logging.info(f"PAY: Duplicate attempt {attempt_id}. Returning stored result.")
    #     return stored_result

    try:
        stored = check_idempotent_operation(idempotency_utf8, idempotency_db_conn) #awaitt
        if stored is not None:
            try:
                if stored is not None:
                    return stored
            except MsgspecDecodeError:
                logging.error("keeps on happening... %s: %s", idempotency_key, e)  
                return {"error": "idempotency data error"}, 500
    except IdempotencyStoreConnectionError as e:
        return {"error": "Idempotency service unavailable"}, 503
    except IdempotencyDataError as e: 
        return {"error": "Idempotency data error"}, 500
    except Exception as e:
        logging.error("keeps on happening... %s: %s", idempotency_key, e)
        return {"error": "Idempotency service error"}, 500 

    def updater(user: UserValue) -> UserValue:
        if user.credit < int(amount):
            raise ValueError(f"User {user_id} has insufficient credit")
        user.credit -= int(amount)
        return user

    # updated_user, error_msg, res_tuple = None, None, None
    updated_user = None
    error_msg = None
    res = None

    try: 
        updated_user, error_msg = await atomic_update_user(user_id, updater) 
    except ValueError as e:
        error_msg = str(e)
        logging.warning(f"'pay' logic failed: {e}")
    except Exception as e:
        logging.exception("internal error")
        error_msg = str(e)

    
    if error_msg:
        logging.error(f"'pay' logic failed: {error_msg}")
        logging.debug(f"error_msg: {error_msg}")
        if "insufficient credit" in error_msg.lower():
            status_code = 400 
        elif "not found" in error_msg.lower(): 
            status_code = 404
        else: 
            status_code = 500
        res = ({"paid": False, "error": error_msg}, status_code)
        try:
            store_idempotent_result(idempotency_key, res, idempotency_db_conn)
        except Exception as e:
            logging.error(f"failure to store failure result ={idempotency_key}: {e}")
        return res
        
    elif updated_user:
        
        logging.info(f" 'pay' logic succeeded: New credit: {updated_user.credit}")
        res= ({"paid": True, "credit": updated_user.credit}, 200)
    
        try:
            was_set = store_idempotent_result(idempotency_key, res, idempotency_db_conn)
            if not was_set:
                # Race condition
                logging.warning(f"Idempotency key {idempotency_key} already existed or failed to set (race condition likely lost).")
                # decide? Raise error for NACK or allow ACK? raising is safer???  state-wise? TODO
        except (IdempotencyDataError, IdempotencyStoreConnectionError) as e:
            logging.critical(f"FAILED TO STORE idempotency result for key {idempotency_key}: {e}")
            # operation finished, but failed to record its state
            # replay if the caller retreis?? for now just return res, with logging, not worryyish
            
        except Exception as e:
            logging.critical("FAILED TO STORE ::", idempotency_key, e)
            # prevent auto-ACK. Manual intervention |0|
        return res
    else:
        #fallback
        res = ({"paid": False, "error": "Internal processing error"}, 500)
        try:
            store_idempotent_result(idempotency_key, res, idempotency_db_conn)
        except Exception as e:
            logging.error(f"Failed to store idempotency result,unkown {idempotency_key}: {e}")
        return res
    
    
@worker.register
async def remove_credit(data):
    user_id = data.get("user_id")
    amount = int(data.get("amount", 0))

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
async def reverse(data):
    user_id = data.get("user_id")
    amount = int(data.get("amount", 0))

    if not user_id or amount <= 0:
        return {"error": "Invalid reverse amount"}, 400

    def updater(user: UserValue) -> UserValue:
        user.credit += amount
        return user

    updated_user, error_msg = await atomic_update_user(user_id, updater)

    if error_msg:
        return {"error": error_msg}, 400
    return {"msg": f"Refunded {amount} to user {user_id}"}, 200



@worker.register
async def cancel_payment(data):
    try:
        user_id = data.get("user_id")
        amount = int(data.get("amount", 0))

        if not all([user_id, amount is not None]):
            logging.error("Missing user_id or amount in cancel_payment request.")
            return {"error": "Missing required fields"}, 400
        
        if amount <= 0:
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
        # return {"refunded": True, "credi 200
        # return Response(f"User: {user_id} credit updated to: {updated_user.credit}", status=200)
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

