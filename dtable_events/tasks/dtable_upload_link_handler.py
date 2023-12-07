# -*- coding: utf-8 -*-
import logging
import time
import json
import stat
from datetime import datetime, timedelta
from threading import Thread, Event

from apscheduler.schedulers.blocking import BlockingScheduler

from seaserv import seafile_api

from dtable_events.app.event_redis import RedisClient, redis_cache
from dtable_events.db import init_db_session_class
from dtable_events.utils import uuid_str_to_36_chars

logger = logging.getLogger(__name__)


class DTableUploadLinkHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient(config)
        self.interval_hours = 6
        self.cache_timeout = 12 * 60 * 60

    def get_cache_key(self, hour):
        return f'public_form_upload_link:{hour}'

    def handle_upload_link(self, event_data):
        dtable_uuid = event_data.get('dtable_uuid')
        repo_id = event_data.get('repo_id')
        parent_dir = event_data.get('parent_dir') or ''
        if 'public/forms' not in parent_dir:
            return
        cache_key = self.get_cache_key((datetime.now().hour+1)%24)
        dtable_uuids_cache = redis_cache.get(cache_key)
        if dtable_uuids_cache:
            try:
                dtable_uuids = json.loads(dtable_uuids_cache)
            except Exception as e:
                logger.warning('key: %s cache invalid', cache_key)
                dtable_uuids = [(dtable_uuid, repo_id)]
            else:
                dtable_uuids.append((dtable_uuid, repo_id))
        else:
            dtable_uuids = [(dtable_uuid, repo_id)]
        redis_cache.set(cache_key, json.dumps(dtable_uuids), self.cache_timeout)

    def listen_redis(self):
        logger.info('Starting handle dtable upload link...')
        subscriber = self._redis_client.get_subscriber('upload-link')
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    data = json.loads(message['data'])
                    try:
                        self.handle_upload_link(data)
                    except Exception as e:
                        logger.error('Handle dtable upload link: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('upload-link')

    def handle_form_timeout_images(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def handle():
            handle_hour = (datetime.now() - timedelta(hours=self.interval_hours)).hour
            cache_key = self.get_cache_key(handle_hour)
            dtable_uuids_cache = redis_cache.get(cache_key)
            if not dtable_uuids_cache:
                return 
            try:
                dtable_uuids = json.loads(dtable_uuids_cache)
            except Exception as e:
                logger.warning('cache: %s loads dtable uuids error: %s', cache_key, e)
                return
            now_timestamp = datetime.now().timestamp()
            logger.debug('start to check key: %s dtable_uuids: %s ', cache_key, len(dtable_uuids))
            for dtable_uuid, repo_id in dtable_uuids:
                try:
                    public_forms_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/public/forms'
                    logger.debug('start to scan repo: %s dtable: %s path: %s', repo_id, dtable_uuid, public_forms_path)
                    dirents = []
                    limit, offset = 1000, 0
                    while True:
                        step_dirents = seafile_api.list_dir_by_path(repo_id, public_forms_path, offset, limit)
                        if not step_dirents:
                            break
                        dirents.extend(step_dirents)
                        if len(step_dirents) < limit:
                            break
                        offset += limit
                    logger.debug('repo: %s dtable: %s path: %s dirents: %s', repo_id, dtable_uuid, public_forms_path, len(dirents))
                    for dirent in dirents:
                        if stat.S_ISDIR(dirent.mode):
                            continue
                        if dirent.mtime and now_timestamp - dirent.mtime > self.interval_hours * 60 * 60:
                            seafile_api.del_file(repo_id, public_forms_path, json.dumps([dirent.obj_name]), '')
                except Exception as e:
                    logger.exception('scan repo: %s dtable: %s path: %s error: %s', repo_id, dtable_uuid, public_forms_path, e)
            redis_cache.delete(cache_key)

        sched.start()

    def run(self):
        logger.info('start to listen upload-link redis and handle form timeout images...')
        Thread(target=self.listen_redis, daemon=True).start()
        Thread(target=self.handle_form_timeout_images, daemon=True).start()
