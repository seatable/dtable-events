# -*- coding: utf-8 -*-
import copy
import json
import logging
import os
import time
import redis

logger = logging.getLogger(__name__)

REDIS_METRIC_KEY = 'metric'

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

        if not (redis_host := os.getenv('REDIS_HOST')):
            if config.has_option('REDIS', 'host'):
                self._host = config.get('REDIS', 'host')
        else:
            self._host = redis_host

        if not (redis_port := os.getenv('REDIS_PORT')):
            if config.has_option('REDIS', 'port'):
                self._port = config.getint('REDIS', 'port')
        else:
            self._port = redis_port
        try:
            self._port = int(self._port)
        except:
            raise ValueError(f'Invalid redis port: {self._port}')
        
        if not (redis_password := os.getenv('REDIS_PASSWORD')):
            if config.has_option('REDIS', 'password'):
                self._password = config.get('REDIS', 'password')
        else:
            self._password = redis_password

    def get_subscriber(self, channel_name):
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
            except redis.AuthenticationError as e:
                logger.critical('connect to redis auth error: %s', e)
                raise e
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber

    def get(self, key):
        return self.connection.get(key)

    def set(self, key, value, timeout=None):
        if not timeout:
            return self.connection.set(key, value)
        else:
            return self.connection.setex(key, timeout, value)

    def delete(self, key):
        return self.connection.delete(key)
    
    def publish(self, channel_name, message):
        return self.connection.publish(channel_name, message)


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
    
    def create_or_update(self, key, value):
        try:
            current_value = self._redis_client.get(key)
            if current_value:
                current_value_dict_copy = copy.deepcopy(json.loads(current_value))
                current_value_dict_copy.update(value)
                self._redis_client.set(key, json.dumps(current_value_dict_copy))
            else:
                self._redis_client.set(key, json.dumps(value))
        except Exception as e:
            logging.error(e)

    def publish(self, channel_name, message):
        return self._redis_client.publish(channel_name, message)

redis_cache = RedisCache()
