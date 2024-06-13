# -*- coding: utf-8 -*-
import logging
import time
import json
from collections import defaultdict
from datetime import datetime
from threading import Thread, Event

from dateutil import parser
from sqlalchemy import text

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.utils import uuid_str_to_32_chars

logger = logging.getLogger(__name__)


class APICallsCounter(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)

    def count_api_gateway(self, info, db_session):
        try:
            info_time = parser.parse(info.get('time'))
        except:
            info_time = datetime.now()
        stats = info.get('stats') or []
        dtable_counts_dict = defaultdict(lambda: defaultdict(lambda: 0))
        org_counts_dict = defaultdict(lambda: defaultdict(lambda: 0))
        owner_ids_dict = defaultdict(lambda: defaultdict(lambda: 0))
        for stat in stats:
            dtable_uuid = stat.get('base')
            api_name = stat.get('name')
            org_id = stat.get('org_id') or -1
            owner_id = stat.get('owner_id') or ''
            count = stat.get('count') or 0
            dtable_counts_dict[uuid_str_to_32_chars(dtable_uuid)][api_name] += count
            if org_id != -1:
                org_counts_dict[org_id][api_name] += count
            else:
                owner_ids_dict[owner_id][api_name] += count
        month = info_time.strftime('%Y-%m')
        updated_at = str(datetime.now())
        step = 100

        values = []
        for dtable_uuid, apis_dict in dtable_counts_dict.items():
            for api_name, count in apis_dict.items():
                values.append(f"('{dtable_uuid}', '{api_name}', {count}, '{month}', '{updated_at}')")
        for i in range(0, len(values), step):
            sql = '''
                INSERT INTO stats_api_gateway_by_base (`dtable_uuid`, `api_name`, `count`, `month`, `updated_at`)
                VALUES %(values)s
                ON DUPLICATE KEY UPDATE `count`=`count`+VALUES(`count`), `updated_at`='%(updated_at)s'
            ''' % {
                'values': ', '.join(values[i: i+step]),
                'updated_at': updated_at
            }
            db_session.execute(text(sql))
            db_session.commit()

        values = []
        for org_id, apis_dict in org_counts_dict.items():
            for api_name, count in apis_dict.items():
                values.append(f"({org_id}, '{api_name}', {count}, '{month}', '{updated_at}')")
        for i in range(0, len(values), step):
            sql = '''
                INSERT INTO stats_api_gateway_by_team (`org_id`, `api_name`, `count`, `month`, `updated_at`)
                VALUES %(values)s
                ON DUPLICATE KEY UPDATE `count`=`count`+VALUES(`count`), `updated_at`='%(updated_at)s'
            ''' % {
                'values': ', '.join(values[i: i+step]),
                'updated_at': updated_at
            }
            db_session.execute(text(sql))
            db_session.commit()

        values = []
        for owner_id, apis_dict in owner_ids_dict.items():
            for api_name, count in apis_dict.items():
                values.append(f"('{owner_id}', '{api_name}', {count}, '{month}', '{updated_at}')")
        for i in range(0, len(values), step):
            sql = '''
                INSERT INTO stats_api_gateway_by_owner (`owner_id`, `api_name`, `count`, `month`, `updated_at`)
                VALUES %(values)s
                ON DUPLICATE KEY UPDATE `count`=`count`+VALUES(`count`), `updated_at`='%(updated_at)s'
            ''' % {
                'values': ', '.join(values[i: i+step]),
                'updated_at': updated_at
            }
            db_session.execute(text(sql))
            db_session.commit()

    def count_api_calls(self, info, db_session):
        component = info.get('component')
        try:
            if component == 'api_gateway':
                self.count_api_gateway(info, db_session)
        except Exception as e:
            logger.exception('stats api_gateway api calls info: %s error: %s', info, e)

    def run(self):
        logger.info('Starting count api calls...')
        subscriber = self._redis_client.get_subscriber('stats_api_calls')

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    msg = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        self.count_api_calls(msg, session)
                    except Exception as e:
                        logger.exception('count api calls msg: %s error: %s', msg, e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('stats_api_calls')
