# -*- coding: utf-8 -*-
import logging
import time
import json
from collections import defaultdict
from datetime import datetime
from threading import Thread, Event

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil import parser, relativedelta
from sqlalchemy import text

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL
from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.utils import uuid_str_to_32_chars
from dtable_events.utils.dtable_web_api import DTableWebAPI

logger = logging.getLogger(__name__)


class APICallsCounter:
    def __init__(self, config):
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self.keep_months = 3  # including this month

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
        month = info_time.strftime('%Y-%m-01')
        updated_at = str(datetime.now())
        step = 100

        values = []
        for dtable_uuid, apis_dict in dtable_counts_dict.items():
            for api_name, count in apis_dict.items():
                values.append(f"('{dtable_uuid}', '{api_name}', {count}, '{month}', '{updated_at}')")
        for i in range(0, len(values), step):
            sql = '''
                INSERT INTO `stats_api_gateway_by_base` (`dtable_uuid`, `api_name`, `count`, `month`, `updated_at`)
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
                INSERT INTO `stats_api_gateway_by_team` (`org_id`, `api_name`, `count`, `month`, `updated_at`)
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
                INSERT INTO `stats_api_gateway_by_owner` (`owner_id`, `api_name`, `count`, `month`, `updated_at`)
                VALUES %(values)s
                ON DUPLICATE KEY UPDATE `count`=`count`+VALUES(`count`), `updated_at`='%(updated_at)s'
            ''' % {
                'values': ', '.join(values[i: i+step]),
                'updated_at': updated_at
            }
            db_session.execute(text(sql))
            db_session.commit()

        # update exceed status
        org_ids = list(org_counts_dict.keys())
        owner_ids = list(owner_ids_dict.keys())
        if org_ids or owner_ids_dict:
            dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
            try:
                dtable_web_api.internal_update_exceed_api_quota(month, org_ids, owner_ids)
            except Exception as e:
                logger.exception('update exceed api quota error: %s', e)

    def count_api_calls(self, info, db_session):
        component = info.get('component')
        try:
            if component == 'api_gateway':
                self.count_api_gateway(info, db_session)
        except Exception as e:
            logger.exception('stats api_gateway api calls info: %s error: %s', info, e)

    def count(self):
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

    def clean(self):
        logger.info('Starting schedule clean api calls...')
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            session = self._db_session_class()
            clean_month = (datetime.now() - relativedelta.relativedelta(months=self.keep_months)).strftime('%Y-%m-01')
            try:
                # clean stats-api-gateway records
                session.execute(text(f"DELETE FROM `stats_api_gateway_by_base` WHERE `month` <= '{clean_month}'"))
                session.execute(text(f"DELETE FROM `stats_api_gateway_by_owner` WHERE `month` <= '{clean_month}'"))
                session.execute(text(f"DELETE FROM `stats_api_gateway_by_team` WHERE `month` <= '{clean_month}'"))
                session.commit()
            except Exception as e:
                logger.exception('clean api calls counter error: %s', e)
            finally:
                session.close()

        sched.start()

    def reset(self):
        logger.info('Starting schedule reset api calls exceed status...')
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', month='*', day='1', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            session = self._db_session_class()
            sql = "DELETE FROM `exceed_api_quota_teams`"
            logger.info('Start to reset exceed_api_quota_teams')
            try:
                session.execute(text(sql))
                session.commit()
            except Exception as e:
                logger.exception('reset api calls exceed status error: %s', e)
            finally:
                session.close()

            try:
                self._redis_client.connection.publish('exceed_api_quota', json.dumps({'changed': True}))
            except Exception as e:
                logger.exception('publish exceed_api_quota error: %s', e)

        sched.start()

    def start(self):
        Thread(target=self.count, daemon=True).start()
        Thread(target=self.clean, daemon=True).start()
        Thread(target=self.reset, daemon=True).start()
