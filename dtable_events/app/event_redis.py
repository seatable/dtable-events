# -*- coding: utf-8 -*-
import copy
import json
import logging
import os
import time
import redis

from dtable_events.app.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)

REDIS_METRIC_KEY = 'metric'

class RedisClient(object):

    def __init__(self, socket_connect_timeout=30, socket_timeout=None,
                 health_check_interval=None, retry_on_timeout=None):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self._parse_config()
        self._connection_kwargs = {
            'host': self._host,
            'port': self._port,
            'password': self._password,
            'socket_timeout': socket_timeout,
            'socket_connect_timeout': socket_connect_timeout,
            'decode_responses': True,
        }
        if health_check_interval is not None:
            self._connection_kwargs['health_check_interval'] = health_check_interval
        if retry_on_timeout is not None:
            self._connection_kwargs['retry_on_timeout'] = retry_on_timeout

        """
        By default, each Redis instance created will in turn create its own connection pool.
        Every caller using redis client will has it's own pool with config caller passed.
        """
        self.connection = self._build_connection()

    def _build_connection(self):
        return redis.Redis(**self._connection_kwargs)

    def reconnect(self):
        try:
            self.connection.connection_pool.disconnect()
        except Exception:
            pass
        self.connection = self._build_connection()
        return self.connection

    def _parse_config(self):

        self._host = REDIS_HOST
        self._port = REDIS_PORT
        self._password = REDIS_PASSWORD

    def get_subscriber(self, channel_name):
        while True:
            try:
                subscriber = self.connection.pubsub(ignore_subscribe_messages=True)
                subscriber.subscribe(channel_name)
                logger.info('redis pubsub success, success subscribe %s', channel_name)
            except redis.AuthenticationError as e:
                logger.critical('connect to redis auth error: %s', e)
                raise e
            except Exception as e:
                logger.error('redis pubsub failed. {} retry after 10s'.format(e))
                time.sleep(10)
            else:
                return subscriber

    def close_subscriber(self, subscriber):
        if not subscriber:
            return
        try:
            subscriber.close()
        except Exception as e:
            logger.debug('close redis subscriber failed: %s', e)

    def refresh_subscriber(self, subscriber, pubsub_channel_name, reason='unknown'):
        logger.warning('reconnect redis pubsub channel=%s reason=%s', pubsub_channel_name, reason)
        self.close_subscriber(subscriber)
        try:
            self.reconnect()
        except Exception as e:
            logger.warning('redis reconnect failed channel=%s error=%s', pubsub_channel_name, e)
        return self.get_subscriber(pubsub_channel_name)

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
        try:
            return self.connection.publish(channel_name, message)
        except Exception as e:
            logger.warning('redis publish failed on %s: %s', channel_name, e)
            self.reconnect()
            return self.connection.publish(channel_name, message)


class RedisCache(object):
    def __init__(self):
        self._redis_client = None

    def init_redis(self):
        self._redis_client = RedisClient()

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
