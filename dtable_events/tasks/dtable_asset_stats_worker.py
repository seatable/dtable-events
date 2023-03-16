# -*- coding: utf-8 -*-
import logging
import time
import json
import stat
import uuid
from datetime import datetime
from threading import Thread, Event

from seaserv import seafile_api

from dtable_events.app.event_redis import RedisClient
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
        updates = ', '.join(["('%s', %s, '%s')" % tuple(dtable_uuid_size + [updated_at]) for dtable_uuid_size in dtable_uuid_sizes[i: i+step]])
        sql = '''
        INSERT INTO dtable_asset_stats(dtable_uuid, size, updated_at) VALUES %s
        ON DUPLICATE KEY UPDATE size=VALUES(size), updated_at=VALUES(updated_at)
        ''' % updates
        try:
            db_session.execute(sql)
            db_session.commit()
        except Exception as e:
            logger.error('update dtable asset assets error: %s', e)


class DTableAssetStatsWorker(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self.interval = 5 * 60  # listen to seafile event for 5 mins and then calc dtable asset storage
        self.last_stat_time = time.time()
        self._redis_client = RedisClient(config)

    def run(self):
        Thread(target=self.listen_seaf_events_and_update, daemon=True).start()
        Thread(target=self.listen_redis_and_update, daemon=True).start()

    def listen_seaf_events_and_update(self):
        logger.info('Starting handle dtable asset stats...')
        repo_id_ctime_dict = {}
        while not self._finished.is_set():
            if repo_id_ctime_dict and time.time() - self.last_stat_time > self.interval:
                Thread(target=self.stat_dtable_asset_storage, args=(repo_id_ctime_dict,), daemon=True).start()
                self.last_stat_time = time.time()
                repo_id_ctime_dict = {}
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
            ctime = msg.get('ctime')
            if not isinstance(ctime, int) or ctime < time.time() - 30 * 60:  # ignore messages half hour ago
                continue
            repo_id = content.split()[1]
            if not is_valid_uuid(repo_id):
                continue
            if repo_id in repo_id_ctime_dict:
                continue
            repo_id_ctime_dict[repo_id] = ctime

    def stat_dtable_asset_storage(self, repo_id_ctime_dict):
        dtable_uuid_sizes = []
        for repo_id, ctime in repo_id_ctime_dict.items():
            logger.debug('start stat repo: %s ctime: %s', repo_id, ctime)
            asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, '/asset')
            if not asset_dir_id:
                continue
            try:
                dirents = seafile_api.list_dir_by_path(repo_id, '/asset', offset=-1, limit=-1)
            except Exception as e:
                logger.error('repo: %s, get dirents error: %s', repo_id, e)
                continue
            for dirent in dirents:
                if not stat.S_ISDIR(dirent.mode):
                    continue
                if not is_valid_uuid(dirent.obj_name):
                    continue
                logger.debug('start stat repo: %s dirent: %s', repo_id, dirent.obj_name)
                if dirent.mtime > ctime - 5:
                    dtable_uuid = dirent.obj_name
                    try:
                        size = seafile_api.get_file_count_info_by_path(repo_id, f'/asset/{dtable_uuid}').size
                        logger.debug('start stat repo: %s dirent: %s size: %s', repo_id, dirent.obj_name, size)
                    except Exception as e:
                        logger.error('get dtable: %s asset error: %s', dtable_uuid, e)
                        continue
                    dtable_uuid_sizes.append([uuid_str_to_32_chars(dtable_uuid), size])
        if not dtable_uuid_sizes:
            return
        logger.debug('totally need to update dtable: %s', len(dtable_uuid_sizes))
        db_session = self._db_session_class()
        try:
            update_dtable_asset_sizes(dtable_uuid_sizes)
        except Exception as e:
            logger.exception(e)
            logger.error('update dtable asset sizes error: %s', e)
        finally:
            db_session.close()

    def listen_redis_and_update(self):
        logger.info('Starting handle table rows count...')
        subscriber = self._redis_client.get_subscriber('stat-asset')
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    dtable_uuid_repo_ids = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        self.stat_dtable_uuids(dtable_uuid_repo_ids, session)
                    except Exception as e:
                        logger.error('Handle table rows count: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('count-rows')

    def stat_dtable_uuids(self, dtable_uuid_repo_ids, db_session):
        dtable_uuid_sizes = []
        for dtable_uuid, repo_id in dtable_uuid_repo_ids:
            try:
                asset_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}'
                asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_path)
                if not asset_dir_id:
                    dtable_uuid_sizes.append([uuid_str_to_32_chars(dtable_uuid), 0])
                size = seafile_api.get_file_count_info_by_path(repo_id, asset_path).size
                dtable_uuid_sizes.append([uuid_str_to_32_chars(dtable_uuid), size])
                logger.debug('redis repo: %s dtable_uuid: %s size: %s', repo_id, dtable_uuid, size)
            except Exception as e:
                logger.exception(e)
                logger.error('check repo: %s dtable: %s asset size error: %s', repo_id, dtable_uuid, e)
        logger.debug('redis totally need to update dtable: %s', len(dtable_uuid_sizes))
        update_dtable_asset_sizes(dtable_uuid_sizes, db_session)
