# -*- coding: utf-8 -*-
import json
import os
import time
import logging
from threading import Lock, Thread, Event
from datetime import datetime, timedelta

from dtable_events.db import init_db_session_class
from dtable_events.app.event_redis import RedisClient
from dtable_events.utils import uuid_str_to_36_chars
from dtable_events.app.config import UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS, UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES

from sqlalchemy import text
from seaserv import seafile_api

logger = logging.getLogger(__name__)

class UniversalAppAutoBackup(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self._lock = Lock()

    def create_snapshot(self, session, app_id, app_version, app_config):
        """
        Create a snapshot for the universal app.
        """
        cmd = """
        INSERT INTO dtable_app_snapshot
          (app_id, notes, app_version, app_config, created_at)
        VALUES
          (:app_id, :notes, :app_version, :app_config, :created_at)
        RETURNING id
        """
        result = session.execute(text(cmd), {
            'app_id':       app_id,
            'notes':        UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES,
            'app_version':  app_version,
            'app_config':   app_config,
            'created_at':   int(time.time()),
        })
        snapshot_id = result.scalar_one()
        session.commit()
        return snapshot_id

    def backup_custom_pages(self, app_id, app_config, repo_id, dtable_uuid, username, snapshot_id):
        app_config = json.loads(app_config)
        app_settings = app_config.get('settings', {})
        app_pages = app_settings.get('pages', [])
        base_dir = '/asset/%s' % uuid_str_to_36_chars(dtable_uuid)
        for page in app_pages:
            page_type = page.get('type', '')
            page_id = page.get('id', '')
            if page_type == 'custom_page':
                content_url = page.get('content_url', '')
                if not content_url:
                    continue

                if 'external-apps' not in content_url:
                    content_path = base_dir + '/external-apps/' + content_url.strip('/')

                else:
                    if base_dir not in content_url:
                        continue

                    base_dir_index = content_url.find(base_dir)
                    content_path = base_dir + '/' + content_url[base_dir_index + len(base_dir):].strip('/')

                src_path = os.path.dirname(content_path)
                content_file_name = os.path.basename(content_path)
                asset_id = seafile_api.get_file_id_by_path(repo_id, content_path)

                dist_path = os.path.join(src_path, 'custom_page_backup_%s' % snapshot_id)
                try:
                    if asset_id:
                        seafile_api.mkdir_with_parents(repo_id, '/', dist_path[1:], '')
                        seafile_api.copy_file(repo_id, src_path, json.dumps([content_file_name]),
                                            repo_id, dist_path, json.dumps([content_file_name]),
                                            username=username, need_progress=0, synchronous=1)
                except Exception as e:
                    logger.warning('fail to backup dtable: %s app: %s custom page: %s error: %s', dtable_uuid,
                                  app_id, page_id, e)
                    continue

    def backup_single_record_pages(self, app_id, app_config, repo_id, dtable_uuid, username, snapshot_id):
        app_config = json.loads(app_config)
        app_settings = app_config.get('settings', {})
        app_pages = app_settings.get('pages', [])
        base_dir = '/asset/%s' % uuid_str_to_36_chars(dtable_uuid)
        for page in app_pages:
            page_type = page.get('type', '')
            page_id = page.get('id', '')
            if page_type == 'single_record_page':
                content_url = page.get('content_url', '')
                if not content_url:
                    continue

                content_path = base_dir + '/external-apps/' + content_url.strip('/')
    
                src_path = os.path.dirname(content_path)
                content_file_name = os.path.basename(content_path)
                dist_path = os.path.join(src_path, 'single_record_page_backup_%s' % snapshot_id)
                asset_id = seafile_api.get_file_id_by_path(repo_id, content_path)
    
                try:
                    if asset_id:
                        seafile_api.mkdir_with_parents(repo_id, '/', dist_path[1:], '')
                        seafile_api.copy_file(repo_id, src_path, json.dumps([content_file_name]),
                                            repo_id, dist_path, json.dumps([content_file_name]),
                                            username=username, need_progress=0, synchronous=1)
                except Exception as e:
                    logger.warning('fail to backup dtable: %s app: %s single record page: %s error: %s', dtable_uuid,
                                    app_id, page_id, e)
                    continue
  
    def should_auto_backup(self, session, app_id, app_version):
        
        now = datetime.now()
        cutoff = now - timedelta(days=UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS)
        threshold_ts = int(cutoff.timestamp())

        sql = """
        SELECT app_version, created_at
          FROM dtable_app_snapshot
         WHERE app_id = :app_id
        ORDER BY created_at DESC
          LIMIT 1
        """
        latest_snapshot = session.execute(text(sql), {'app_id': app_id}).first()
        if latest_snapshot:
            latest_version, latest_created_at = latest_snapshot
            if latest_version == app_version or latest_created_at > threshold_ts:
                return False
        return True

    def run(self):
        logger.info('Starting universal app auto backup thread')
        subscriber = self._redis_client.get_subscriber('universal-app-auto-backup')
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    msg = json.loads(message['data'])
                    user_name = msg.get('username', '')
                    app_id = msg.get('app_id', '')
                    app_version = msg.get('app_version', '')
                    repo_id = msg.get('repo_id', '')
                    dtable_uuid = msg.get('dtable_uuid', '')
                    app_config = msg.get('app_config', '{}')
                    session = self._db_session_class()
                    try:
                        if not self.should_auto_backup(session, app_id, app_version):
                            continue
                        with self._lock:
                            new_snapshot_id = self.create_snapshot(session, app_id, app_version, app_config)
                            self.backup_custom_pages(app_id, app_config, repo_id, dtable_uuid, user_name, new_snapshot_id)
                            self.backup_single_record_pages(app_id, app_config, repo_id, dtable_uuid, user_name, new_snapshot_id)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('universal-app-auto-backup')
