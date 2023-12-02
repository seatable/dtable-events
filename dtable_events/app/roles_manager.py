import json
import logging
import threading
import time

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL
from dtable_events.app.event_redis import RedisClient, redis_cache
from dtable_events.db import create_engine_from_conf
from dtable_events.utils.dtable_web_api import DTableWebAPI

logger = logging.getLogger(__name__)


class RolesManager:

    def __init__(self, config):
        self.session_class = create_engine_from_conf(config)
        self._redis_client = RedisClient(config)

    def handler(self):
        subscriber = self._redis_client.get_subscriber('role-updated')
        while True:
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    username = event.get('username')
                    org_id = event.get('org_id')
                    if username:
                        redis_cache.delete(f'user_or_org:role:{str(username)}')
                        redis_cache.delete(f'user_or_org:trigger_limit:{str(username)}')
                        redis_cache.delete(f'user_or_org:trigger_count:{str(username)}')
                    if org_id:
                        redis_cache.delete(f'user_or_org:role:{str(org_id)}')
                        redis_cache.delete(f'user_or_org:trigger_limit:{str(org_id)}')
                        redis_cache.delete(f'user_or_org:trigger_count:{str(org_id)}')
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get notification rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('role-updated')

    def on_start(self):
        dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
        while True:
            try:
                roles = dtable_web_api.internal_roles()['rows']
                redis_cache.set('roles', roles)
            except:
                time.sleep(5)
        

    def start(self):
        threading.Thread(target=self.handler, daemon=True)
