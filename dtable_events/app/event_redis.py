# -*- coding: utf-8 -*-
import logging
import time
import redis

logger = logging.getLogger(__name__)


class RedisClient(object):

    def __init__(self, config, socket_connect_timeout=30, socket_timeout=None):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self._parse_config(config)

        """
        By default, each Redis instance created will in turn create its own connection pool.
        Every caller using redis client will has it's own pool with config caller passed.
        """
        self.connection = redis.Redis(
            host=self._host, port=self._port, password=self._password,
            socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
            decode_responses=True
            )

    def _parse_config(self, config):
        if config.has_option('REDIS', 'host'):
            self._host = config.get('REDIS', 'host')

        if config.has_option('REDIS', 'port'):
            self._port = config.getint('REDIS', 'port')

        if config.has_option('REDIS', 'password'):
            self._password = config.get('REDIS', 'password')

    def auth_check(self):
        try:
            return self.connection.ping()
        except redis.AuthenticationError as e:
            logger.critical('connect to redis auth error: %s', e)
            return None

    def get_subscriber(self, channel_name):
        if not self.auth_check():
            raise redis.AuthenticationError('redis username or password invalid')
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber

    def get(self, key):
        if not self.auth_check():
            logger.error('redis username or password invalid')
            return None
        return self.connection.get(key)

    def set(self, key, value, timeout=None):
        if not self.auth_check():
            logger.error('redis username or password invalid')
            return None
        if not timeout:
            return self.connection.set(key, value)
        else:
            return self.connection.setex(key, timeout, value)

    def delete(self, key):
        if not self.auth_check():
            logger.error('redis username or password invalid')
            return None
        return self.connection.delete(key)


class RedisCache(object):
    def __init__(self):
        self._redis_client = None

    def init_redis(self, config):
        self._redis_client = RedisClient(config)

    def get(self, key):
        return self._redis_client.get(key)

    def set(self, key, value, timeout=None):
        return self._redis_client.set(key, value, timeout=timeout)

    def delete(self, key):
        return self._redis_client.delete(key)


redis_cache = RedisCache()
