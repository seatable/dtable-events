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


def update_org_big_data_storage_stats(db_session, uuid_stats_map):
    # {workspace_id1: [uuid1, uuid2], workspace_id2: [uuid3], workspace_id3: [uuid4]}
    workspace_id_uuid_map = dict()
    sql1 = """SELECT workspace_id, uuid FROM dtables WHERE uuid IN :uuid_list"""
    results1 = db_session.execute(sql1, {'uuid_list': list(uuid_stats_map.keys())}).fetchall()
    for result in results1:
        if not workspace_id_uuid_map.get(result[0]):
            workspace_id_uuid_map[result[0]] = [result[1]]
        else:
            workspace_id_uuid_map[result[0]].append(result[1])

    # {org_id1: [uuid1, uuid2, uuid3], org_id2: [uuid4]}
    org_id_uuid_map = dict()
    sql2 = """SELECT id, org_id FROM workspaces WHERE org_id != -1 AND id IN :workspace_id_list"""
    results2 = db_session.execute(sql2, {'workspace_id_list': list(workspace_id_uuid_map.keys())}).fetchall()
    for result in results2:
        if not org_id_uuid_map.get(result[1]):
            org_id_uuid_map[result[1]] = workspace_id_uuid_map[result[0]]
        else:
            org_id_uuid_map[result[1]].append(workspace_id_uuid_map[result[0]])

    # {org_id1: {rows: rows1, storage: storage1}, org_id2: {rows: rows2, storage: storage2}}
    org_id_stats_map = dict()
    for org_id, uuid_list in org_id_uuid_map.items():
        org_id_stats_map[org_id] = {'rows': 0, 'storage': 0}
        for uuid in uuid_list:
            org_id_stats_map[org_id]['rows'] += int(uuid_stats_map[uuid]['rows'])
            org_id_stats_map[org_id]['storage'] += int(uuid_stats_map[uuid]['storage'])

    if org_id_stats_map:
        sql3 = "REPLACE INTO org_big_data_storage_stats (org_id, total_rows, total_storage) VALUES %s" % ', '.join([
            "('%s', '%s', '%s')" % (org_id, stats.get('rows'), stats.get('storage'))
            for org_id, stats in org_id_stats_map.items()])
        db_session.execute(sql3)
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

            # {uuid1: {rows: rows1, storage: storage1}, uuid2: {rows: rows2, storage: storage2}}
            uuid_stats_map = dict()
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
                    for base in bases:
                        dtable_uuid = uuid.UUID(base.get('id')).hex
                        uuid_stats_map[dtable_uuid] = {'rows': base.get('rows'), 'storage': base.get('storage')}
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

            if uuid_stats_map:
                session = self.db_session_class()
                try:
                    update_org_big_data_storage_stats(session, uuid_stats_map)
                except Exception as e:
                    logging.error(e)
                finally:
                    session.close()

        schedule.start()
