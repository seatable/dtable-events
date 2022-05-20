# -*- coding: utf-8 -*-
import os
import sys
import time
import logging
from threading import Thread

import jwt
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.db import init_db_session_class


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')
sys.path.insert(0, dtable_web_dir)

try:
    from seahub.settings import INNER_DTABLE_DB_URL
    from seahub.settings import DTABLE_PRIVATE_KEY
except ImportError as err:
    INNER_DTABLE_DB_URL = ''
    DTABLE_PRIVATE_KEY = ''
    logging.warning('Can not import seahub.settings: %s.' % err)

__all__ = [
    'BigDataStorageStatsWorker',
]


def update_big_data_storage_stats(db_session, bases):
    sql = "REPLACE INTO big_data_storage_stats (dtable_uuid, total_rows, total_storage) VALUES %s" % \
          ', '.join(["('%s', '%s', '%s')" % (base.get('id'), base.get('rows'), base.get('storage')) for base in bases])
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

        schedule.start()
