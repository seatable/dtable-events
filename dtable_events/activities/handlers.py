# -*- coding: utf-8 -*-
import logging
import time
import json
from threading import Thread, Event
from json.decoder import JSONDecodeError
from dtable_events.app.event_redis import RedisClient
from dtable_events.activities.db import save_or_update_or_delete, cache_dtable_update_info
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
        'modify_rows',
        'add_link',
        'remove_link',
        'update_links',
        'update_rows_links'
    ]

    def __init__(self, app):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class()
        self._redis_client = RedisClient(socket_connect_timeout=5, socket_timeout=5,
                                         health_check_interval=30, retry_on_timeout=True)
        self.app = app
        self._pubsub_channel_name = 'table-events'
        self._pubsub_no_message_timeout = 5 * 60

    def run(self):
        logger.info('Starting handle table activities...')
        subscriber = self._redis_client.get_subscriber(self._pubsub_channel_name)
        last_pubsub_message_time = time.time()
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    if message.get('type') != 'message':
                        continue
                    last_pubsub_message_time = time.time()
                    event = json.loads(message['data'])
                    if event['op_type'] not in self.SUPPORT_OPERATION_TYPES:
                        continue
                    session = self._db_session_class()
                    try:
                        save_or_update_or_delete(session, event)
                        cache_dtable_update_info(self.app, event)
                    except JSONDecodeError as err:
                        logger.warning('Json decode error on handling activity messages: %s' % err)
                    except Exception as e:
                        logger.exception(e)
                        logger.error('Handle activities message failed: %s' % e)
                    finally:
                        session.close()
                else:
                    if (time.time() - last_pubsub_message_time) >= self._pubsub_no_message_timeout:
                        subscriber = self._redis_client.refresh_subscriber(
                            subscriber, self._pubsub_channel_name, 'no message timeout')
                        last_pubsub_message_time = time.time()
                        continue
                    time.sleep(0.5)
            except Exception as e:
                logger.error('redis pubsub receive error: %s', e)
                subscriber = self._redis_client.refresh_subscriber(subscriber, self._pubsub_channel_name, str(e))
                last_pubsub_message_time = time.time()
