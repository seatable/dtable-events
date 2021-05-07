# -*- coding: utf-8 -*-
import logging
import time
import json
from threading import Thread, Event

from dtable_events.app.event_redis import RedisClient
from dtable_events.activities.db import save_or_update_or_delete
from dtable_events.db import init_db_session_class
from dtable_events.activities.notification_rules_utils import scan_notifications_rules_per_update
from dtable_events.cache.clients import RedisCacheClient

logger = logging.getLogger(__name__)


class MessageHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self._cache = RedisCacheClient(self._redis_client)

    def run(self):
        logger.info('Starting handle table activities...')
        subscriber = self._redis_client.get_subscriber('table-events')
        
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        save_or_update_or_delete(session, event)
                        if event.get('op_type') == 'modify_row':
                            scan_notifications_rules_per_update(event, db_session=session, cache=self._cache)
                    except Exception as e:
                        logger.error('Handle activities message failed: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('table-events')
