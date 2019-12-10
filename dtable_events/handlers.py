# -*- coding: utf-8 -*-
import os
import sys
import logging
import time
import json
from threading import Thread, Event

from dtable_events.event_redis import event_redis
from dtable_events.models import Activities, UserActivities
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def _redis_connection(config):
    while True:
        try:
            connection = event_redis.get_connection(config)
            connection.ping()
        except Exception as e:
            logger.error('redis error: %s, reconnecting', e)
            time.sleep(5)
        else:
            return connection


def _save_user_activities(session, event):
    dtable_uuid = event['dtable_uuid']
    row_id = event['row_id']
    op_user = event['op_user']
    op_type = event['op_type']

    local_time = time.localtime(int(event['op_time']) / 1000)
    op_time = time.strftime('%Y-%m-%d %H:%M:%S', local_time)

    table_id = event['table_id']
    table_name = event['table_name']
    row_data = event['row_data']

    detail_dict = dict()
    detail_dict["table_id"] = table_id
    detail_dict["table_name"] = table_name
    detail_dict["row_data"] = row_data
    detail = json.dumps(detail_dict)

    activity = Activities(dtable_uuid, row_id, op_user, op_type, op_time, detail)
    session.add(activity)
    session.commit()

    dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
    if not dtable_web_dir:
        logger.critical('dtable_web_dir is not set')
        raise RuntimeError('dtable_web_dir is not set')

    if not os.path.exists(dtable_web_dir):
        logger.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
        raise RuntimeError('dtable_web_dir %s does not exist' % dtable_web_dir)

    sys.path.insert(0, dtable_web_dir)
    try:
        from seahub.dtable.utils import list_dtable_related_users
        from seahub.dtable.models import DTables
    except ImportError:
        logger.critical('Can not import dtable_web\'s module')
        raise RuntimeError('Can not import dtable_web\'s module')

    dtable = DTables.objects.get_dtable_by_uuid(dtable_uuid)
    user_list = list_dtable_related_users(dtable.workspace, dtable)

    if op_user not in user_list:
        user_list = user_list + [op_user]

    for user in user_list:
        user_activity = UserActivities(user, activity.id, activity.op_time)
        session.add(user_activity)
    session.commit()


class MessageHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.finished = Event()
        self._redis_connection = _redis_connection(config)
        self._subscriber = self._redis_connection.pubsub(ignore_subscribe_messages=True)
        self._subscriber.subscribe('dtable_activities')

    def run(self):
        logger.info('Starting handle message...')
        while not self.finished.is_set():
            try:
                message = self._subscriber.get_message()
                if message is not None:
                    event = message['data']
                    self._redis_connection.rpush('table_event_queue', event)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)


class EventHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.finished = Event()
        self._redis_connection = _redis_connection(config)
        self._db_session_class = init_db_session_class(config)

    def run(self):
        logger.info('Starting handle event...')
        while not self.finished.is_set():
            try:
                event_tuple = self._redis_connection.blpop('table_event_queue', 1)
                if event_tuple is not None:
                    key, value = event_tuple
                    event = json.loads(value)

                    session = self._db_session_class()
                    try:
                        _save_user_activities(session, event)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
            except Exception as e:
                logger.error('Failed handle message: %s' % e)
