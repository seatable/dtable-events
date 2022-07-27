# -*- coding: utf-8 -*-
import os
import sys
import time
import uuid
import logging
from threading import Thread

import jwt
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import INNER_DTABLE_DB_URL, DTABLE_PRIVATE_KEY
from dtable_events.db import init_db_session_class

__all__ = [
    'BigDataStorageStatsWorker',
]


def update_big_data_storage_stats(db_session, bases):
    uuid_org_id_map = dict()
    get_org_id_sql = """SELECT uuid, org_id FROM dtables d JOIN workspaces w ON d.workspace_id=w.id
                        WHERE uuid IN :uuid_list"""
    results = db_session.execute(get_org_id_sql,
                                 {'uuid_list': [uuid.UUID(base.get('id')).hex for base in bases]}).fetchall()
    for result in results:
        uuid_org_id_map[result[0]] = result[1]

    sql = "REPLACE INTO big_data_storage_stats (dtable_uuid, total_rows, total_storage, org_id) VALUES %s" % ', '.join(
        ["('%s', '%s', '%s', '%s')" % (base.get('id'), base.get('rows'), base.get('storage'),
                                       uuid_org_id_map.get(uuid.UUID(base.get('id')).hex, -1)) for base in bases])
    db_session.execute(sql)
    db_session.commit()


def update_org_big_data_storage_stats(db_session):
    get_stats_sql = """SELECT org_id, SUM(total_rows) AS total_rows, SUM(total_storage) AS total_storage
                       FROM big_data_storage_stats WHERE org_id != -1 GROUP BY org_id"""
    results = db_session.execute(get_stats_sql).fetchall()

    if results:
        sql = "REPLACE INTO org_big_data_storage_stats (org_id, total_rows, total_storage) VALUES %s" % \
              ', '.join(["('%s', '%s', '%s')" % (res[0], res[1], res[2]) for res in results])
        db_session.execute(sql)
        db_session.commit()


class BigDataStorageStatsWorker(object):

    def __init__(self, config):
        self._logfile = None
        self._db_session_class = init_db_session_class(config)
        self._prepare_logfile()

    def _prepare_logfile(self):
        log_dir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(log_dir, 'big_data_storage_stats.log')

    def start(self):
        logging.info('Start big data storage stats worker.')
        BigDataStorageStatsTask(self._db_session_class).start()


class BigDataStorageStatsTask(Thread):
    def __init__(self, db_session_class):
        super(BigDataStorageStatsTask, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        schedule = BlockingScheduler()
        # run at 1 o'clock in every day of week
        @schedule.scheduled_job('cron', day_of_week='*', hour='1')
        def timed_job():
            logging.info('Start big data storage stats task...')

            offset = 0
            limit = 1000
            while 1:
                api_url = INNER_DTABLE_DB_URL.rstrip('/') + '/api/v1/bases/?offset=%s&limit=%s' % (offset, limit)
                headers = {'Authorization': 'Token ' + jwt.encode({
                    'is_db_admin': True, 'exp': int(time.time()) + 60,
                }, DTABLE_PRIVATE_KEY, 'HS256')}
                try:
                    resp = requests.get(api_url, headers=headers).json()
                    bases = resp.get('bases', []) if resp else []
                    if len(bases) > 0:
                        db_session = self.db_session_class()
                        try:
                            update_big_data_storage_stats(db_session, bases)
                        except Exception as e:
                            logging.error(e)
                        finally:
                            db_session.close()
                        offset += limit
                    else:
                        break
                except Exception as e:
                    logging.error(e)
                    break

            session = self.db_session_class()
            try:
                update_org_big_data_storage_stats(session)
            except Exception as e:
                logging.error(e)
            finally:
                session.close()

        schedule.start()
