import asyncio
import logging
import time

from redis.asyncio.sentinel import Sentinel
import redis  # Standard Redis for exceptions
import redis.asyncio as aioredis  # Async Redis as a different name
from redis.asyncio.sentinel import Sentinel


class RedisPoolManager:
    _instance = None
    _pools = {}
    _initialized = False
    _lock = asyncio.Lock()
    _masters = {}


    def __init__(self):
        self.sentinel = None
        self._masters = {}

    @classmethod
    async def get_instance(cls, sentinel_hosts,  password, db=0, force_refresh=False):
        async with cls._lock:
            if not cls._initialized:
                cls._instance = cls()
                cls._instance.sentinel = Sentinel(
                    sentinel_hosts,
                    socket_timeout=10,
                    socket_connect_timeout=5,
                    socket_keepalive=True,
                    password=password,
                    retry_on_timeout=True
                )
                cls._initialized = True
                cls._instance.masters={}
                logging.info("Redis Pool Manager initialized")
            return cls._instance

    async def get_master(self, service_name, decode_responses=False, db=0):
        if self.sentinel is None:
            raise RuntimeError("Sentinel not initialized")

        pool_key = f"master:{service_name}:{decode_responses}:{db}"
        try:
            refresh_needed = pool_key not in self.__class__._pools or time.time() - self.__class__._last_refresh > 30

            if refresh_needed:
                master_pool = self.sentinel.master_for(
                    service_name,
                    decode_responses=decode_responses,
                    db=db
                )

                try:
                    # Test the connection - this will fail if the master is down
                    await master_pool.ping()
                    self.__class__._pools[pool_key] = master_pool
                    logging.info(f"Successfully connected to master for {service_name}")
                except Exception as e:
                    logging.warning(f"Master ping failed for {service_name}: {e}")
                    # Force sentinel to rediscover masters - THIS IS THE KEY PART FOR FAILOVER
                    await self.refresh_sentinel_connections()
                    # Try again with the new master topology
                    master_pool = self.sentinel.master_for(
                        service_name,
                        decode_responses=decode_responses,
                        db=db
                    )
                    await master_pool.ping()  # Verify the new connection
                    self.__class__._pools[pool_key] = master_pool
                    logging.info(f"Reconnected to new master for {service_name} after failover")

            # Return the current master - should be the promoted replica if failover occurred
            return self.__class__._pools[pool_key]
        except Exception as e:
            logging.error(f"Error getting master for {service_name}: {e}")
            # Try to refresh all sentinel connections as a last resort
            await self.refresh_sentinel_connections()
            # Return a fresh connection even if it's not in our pool
            master_pool = self.sentinel.master_for(
                service_name,
                decode_responses=decode_responses,
                db=db
            )
            self.__class__._pools[pool_key] = master_pool
            return master_pool

    async def get_slave(self, service_name, decode_responses=False, db=0):
        if self.sentinel is None:
            raise RuntimeError("Sentinel not initialized")
        pool_key = f"slave:{service_name}:{decode_responses}:{db}"
        if pool_key not in self._pools:
            self._pools[pool_key] = self.sentinel.slave_for(
                service_name,
                decode_responses=decode_responses,
                db=db
            )
        return self.__class__._pools[pool_key]

    async def refresh_sentinel_connections(self):
        current_time = time.time()
        if current_time - self.__class__._last_refresh < 5:
            return

        self.__class__._last_refresh = current_time

        try:
            for service_name in ['order', 'payment', 'stock', 'saga']:
                try:
                    # This asks Sentinel for the current master - critical for failover
                    new_master_info = await self.sentinel.discover_master(service_name)
                    logging.info(f"Current master for {service_name}: {new_master_info}")

                    # Check if master has changed - indicates failover
                    old_master = self._masters.get(service_name)
                    if old_master != new_master_info:
                        logging.warning(
                            f"Master change detected for {service_name}! Old: {old_master}, New: {new_master_info}")
                        # Update our knowledge of the master
                        self._masters[service_name] = new_master_info

                        # Close and recreate all connections for this service
                        for prefix in ['master', 'slave']:
                            for decode_responses in [True, False]:
                                for db in range(3):  # Common db numbers
                                    pool_key = f"{prefix}:{service_name}:{decode_responses}:{db}"
                                    if pool_key in self.__class__._pools:
                                        # Close old connection
                                        try:
                                            await self.__class__._pools[pool_key].close()
                                        except Exception as e:
                                            logging.error(f"Error closing old connection: {e}")

                                        # Create new connection pointing to the new master/replica
                                        if prefix == 'master':
                                            self.__class__._pools[pool_key] = self.sentinel.master_for(
                                                service_name,
                                                decode_responses=decode_responses,
                                                db=db
                                            )
                                        else:
                                            self.__class__._pools[pool_key] = self.sentinel.slave_for(
                                                service_name,
                                                decode_responses=decode_responses,
                                                db=db
                                            )
                except Exception as e:
                    logging.error(f"Error discovering master for {service_name}: {e}")

            logging.info("Sentinel connections refreshed successfully")
        except Exception as e:
            logging.error(f"Error refreshing sentinel connections: {e}")

    async def refresh_master_connection(self, service_name):
        """Force a refresh of the master connection for the given service"""
        if self.sentinel is None:
            raise RuntimeError("Sentinel not initialized")
        try:
          master_info = await self.sentinel.discover_master(f"{service_name}-master")
        except Exception as e:
            logging.error(f"Error discovering master for {service_name}: {e}")
            master_info = None

        # Get the current master info and update internal connection
        self._masters[service_name] = master_info
        pool_key = f"master:{service_name}:False:0"  # Default pool key
        if pool_key in self.__class__._pools:
            # Get a fresh connection from sentinel
            self.__class__._pools[pool_key] = self.sentinel.master_for(
                service_name,
                decode_responses=False,
                db=0
            )

        return master_info

    async def reset_connections(self):
        """Reset all connections to ensure we're using current masters"""
        for service_name in ['order', 'saga']:
            try:
                await self.refresh_master_connection(service_name)
            except Exception as e:
                logging.error(f"Error refreshing master for {service_name}: {e}")

    async def close_all(self):
        for pool_key, pool in self._pools.items():
            try:
                await pool.close()
            except Exception as e:
                logging.error(f"Error closing pool {pool_key}: {e}")
        self._pools.clear()


# Add this to redis_pool_manager.py
class IdempotencyRedisProxy:
    _instance = None
    _initialized = False
    _client = None
    _pool_manager = None
    _service_name = None
    _db = 0

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def configure(cls, service_name, db=0):
        cls._service_name = service_name
        cls._db = db

    @classmethod
    async def initialize(cls, pool_manager):
        if not cls._initialized:
            cls._pool_manager = pool_manager
            cls._client = await pool_manager.get_master(
                cls._service_name,
                decode_responses=False,
                db=cls._db
            )
            cls._initialized = True
            logging.info(f"Idempotency Redis client initialized for {cls._service_name} db={cls._db}")
        return cls._client

    def __getattr__(self, name):
        if self._client is None:
            raise RuntimeError("Idempotency Redis client not initialized. Make sure to call initialize() first")
        return getattr(self._client, name)