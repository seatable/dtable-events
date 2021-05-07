from dtable_events.app.event_redis import RedisClient


class BaseCacheClient:

    def set(self, key, value, timeout=None):
        raise NotImplementedError('.set() must be overridden')

    def get(self, key):
        raise NotImplementedError('.get() must be overridden')

    def delete(self, key):
        raise NotImplementedError('.delete() must be overridden')


class RedisCacheClient(BaseCacheClient):

    def __init__(self, redis_client: RedisClient):
        self._redis_client = redis_client

    def set(self, key, value, timeout=None):
        if not timeout:
            self._redis_client.connection.set(key, value, timeout=timeout)
        else:
            self._redis_client.connection.setex(key, timeout, value)

    def get(self, key):
        self._redis_client.connection.get(key)

    def delete(self, key):
        self._redis_client.delete(key)

