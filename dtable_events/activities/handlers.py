# -*- coding: utf-8 -*-
import logging
import time
import json
from threading import Thread, Event

from dtable_events.app.event_redis import RedisClient
from dtable_events.activities.db import save_or_update_or_delete
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


class MessageHandler(Thread):
    SUPPORT_OPERATION_TYPES = [
        'insert_row',
        'insert_rows',
        'append_rows',
        'delete_row',
        'delete_rows',
        'modify_row',
        'modify_rows'
    ]

    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)

    def run(self):
        logger.info('Starting handle table activities...')
        subscriber = self._redis_client.get_subscriber('table-events')
        
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    if event['op_type'] not in self.SUPPORT_OPERATION_TYPES:
                        continue
                    session = self._db_session_class()
                    try:
                        save_or_update_or_delete(session, event)
                    except Exception as e:
                        logger.exception(e)
                        logger.error('Handle activities message failed: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('table-events')
