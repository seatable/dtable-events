# -*- coding: utf-8 -*-
import logging
import time
import json
from threading import Thread, Event

from seaserv import ccnet_api

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

    cmd = "SELECT to_user FROM dtable_share WHERE dtable_id=(SELECT id FROM dtables WHERE uuid=:dtable_uuid)"
    user_list = [res[r'to_user'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})]

    cmd = "SELECT owner FROM workspaces WHERE id=(SELECT workspace_id FROM dtables WHERE uuid=:dtable_uuid)"
    owner = [res[r'owner'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})][0]

    if '@seafile_group' not in owner:
        user_list.append(op_user)
    else:
        group_id = int(owner.split('@')[0])
        members = ccnet_api.get_group_members(group_id)
        for member in members:
            if member.user_name not in user_list:
                user_list.append(member.user_name)

    for user in user_list:
        user_activity = UserActivities(activity.id, user, activity.op_time)
        session.add(user_activity)
    session.commit()


class MessageHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_connection = _redis_connection(config)
        self._subscriber = self._redis_connection.pubsub(ignore_subscribe_messages=True)
        self._subscriber.subscribe('dtable_activities')

    def run(self):
        logger.info('Starting handle message...')
        while not self._finished.is_set():
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
        self._finished = Event()
        self._redis_connection = _redis_connection(config)
        self._db_session_class = init_db_session_class(config)

    def run(self):
        logger.info('Starting handle event...')
        while not self._finished.is_set():
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
