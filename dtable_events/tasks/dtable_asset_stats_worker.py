# -*- coding: utf-8 -*-
import logging
import time
import json
import stat
import uuid
from datetime import datetime
from threading import Thread, Event

from seaserv import seafile_api

from dtable_events.db import init_db_session_class
from dtable_events.utils import uuid_str_to_36_chars, uuid_str_to_32_chars

logger = logging.getLogger(__name__)


def is_valid_uuid(test_str):
    try:
        uuid.UUID(test_str)
        return True
    except:
        return False


def update_dtable_asset_sizes(dtable_uuid_sizes, db_session):
    """
    :param dtable_uuid_sizes: a list of [dtable_uuid, size]
    """
    step = 1000
    updated_at = datetime.utcnow()
    for i in range(0, len(dtable_uuid_sizes), step):
        updates = ', '.join(["('%s', %s, '%s')" % (
            uuid_str_to_32_chars(dtable_uuid_size[0]), dtable_uuid_size[1], updated_at
        ) for dtable_uuid_size in dtable_uuid_sizes[i: i+step]])
        sql = '''
        INSERT INTO dtable_asset_stats(dtable_uuid, size, updated_at) VALUES %s
        ON DUPLICATE KEY UPDATE size=VALUES(size), updated_at=VALUES(updated_at)
        ''' % updates
        db_session.execute(sql)
        db_session.commit()


class DTableAssetStatsWorker(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self.interval = 5 * 60  # listen to seafile event for some time and then calc dtable asset storage
        self.last_stats_time = time.time()
        self.flag_worker_running = False

    def run(self):
        logger.info('Starting handle dtable asset stats...')
        repo_id_ctime_dict = {}
        while not self._finished.is_set():
            if not self.flag_worker_running and repo_id_ctime_dict and time.time() - self.last_stats_time > self.interval:
                Thread(target=self.stats_dtable_asset_storage, args=(repo_id_ctime_dict,), daemon=True).start()
                self.last_stats_time = time.time()
                repo_id_ctime_dict = {}
                self.flag_worker_running = True
            msg = seafile_api.pop_event('seaf_server.event')
            if not msg:
                time.sleep(0.5)
                continue
            logger.debug('msg: %s', msg)
            if not isinstance(msg, dict):
                continue
            content = msg.get('content')
            if not isinstance(content, str) or '\t' not in content:
                continue
            if not content.startswith('repo-update'):
                continue
            ctime = msg.get('ctime')
            if not isinstance(ctime, int) or ctime < time.time() - 30 * 60:  # ignore messages half hour ago
                continue
            repo_id = content.split()[1]
            if not is_valid_uuid(repo_id):
                continue
            if repo_id in repo_id_ctime_dict:
                continue
            repo_id_ctime_dict[repo_id] = ctime

    def _stats_dtable_asset_storage(self, repo_id_ctime_dict):
        logger.info('Starting stats repo dtable asset storage...')
        dtable_uuid_sizes = []
        for repo_id, ctime in repo_id_ctime_dict.items():
            logger.debug('start stats repo: %s ctime: %s', repo_id, ctime)
            try:
                repo = seafile_api.get_repo(repo_id)
                if not repo:
                    continue
                asset_dirent = seafile_api.get_dirent_by_path(repo_id, '/asset')
                if not asset_dirent or asset_dirent.mtime < ctime:
                    continue
                dirents = seafile_api.list_dir_by_path(repo_id, '/asset', offset=-1, limit=-1)
                for dirent in dirents:
                    if not stat.S_ISDIR(dirent.mode):
                        continue
                    if not is_valid_uuid(dirent.obj_name):
                        continue
                    logger.debug('start stats repo: %s dirent: %s', repo_id, dirent.obj_name)
                    if dirent.mtime >= ctime:
                        dtable_uuid = dirent.obj_name
                        size = seafile_api.get_file_count_info_by_path(repo_id, f'/asset/{dtable_uuid}').size
                        logger.debug('start stats repo: %s dirent: %s size: %s', repo_id, dirent.obj_name, size)
                        dtable_uuid_sizes.append([uuid_str_to_32_chars(dtable_uuid), size])
            except Exception as e:
                logger.exception(e)
                logger.error('stats repo: %s error: %s', repo_id, e)
        if not dtable_uuid_sizes:
            return
        logger.debug('totally need to update dtable: %s', len(dtable_uuid_sizes))
        db_session = self._db_session_class()
        try:
            update_dtable_asset_sizes(dtable_uuid_sizes, db_session)
        except Exception as e:
            logger.exception(e)
            logger.error('update dtable asset sizes error: %s', e)
        finally:
            db_session.close()

    def stats_dtable_asset_storage(self, repo_id_ctime_dict):
        try:
            self._stats_dtable_asset_storage(repo_id_ctime_dict)
        except Exception as e:
            logger.exception(e)
        finally:
            self.flag_worker_running = False
