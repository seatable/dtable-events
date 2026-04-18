# -*- coding: utf-8 -*-
import json
import time
import logging
from threading import Thread, Event

from dtable_events.db import init_db_session_class
from dtable_events.app.event_redis import RedisClient
from dtable_events.statistics.db import save_user_activity_stat

logger = logging.getLogger(__name__)


class UserActivityCounter(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class()
        self._redis_client = RedisClient()

    def run(self):
        logger.info('Starting count user activity...')
        subscriber = self._redis_client.get_subscriber('user-activity-statistic')

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    msg = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        save_user_activity_stat(session, msg)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('user-activity-statistic')
