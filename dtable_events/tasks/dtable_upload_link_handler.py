# -*- coding: utf-8 -*-
import logging
import stat
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from seaserv import seafile_api

from dtable_events.db import init_db_session_class
from dtable_events.utils import uuid_str_to_36_chars

logger = logging.getLogger(__name__)


class DTableUploadLinkHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self.session_class = init_db_session_class(config)
        self.interval_hours = 6

    def handle_flags(self, session):
        now = datetime.now()
        flag_time = (now - timedelta(hours=self.interval_hours)).replace(minute=0, second=0, microsecond=0)
        offset, limit = 0, 1000
        while True:
            sql = "SELECT dtable_uuid, repo_id FROM dtable_form_upload_link_flags WHERE flag_time<=:flag_time LIMIT :offset, :limit"
            try:
                results = list(session.execute(text(sql), {'flag_time': flag_time, 'offset': offset, 'limit': limit}))
            except Exception as e:
                logger.error('query upload flags flag_time: %s error: %s', flag_time, e)
                break
            logger.debug('flag_time: %s offset: %s limit: %s query results: %s', flag_time, offset, limit, len(results))
            repo_id_dtable_uuids_dict = defaultdict(list)
            for dtable_uuid, repo_id in results:
                repo_id_dtable_uuids_dict[repo_id].append(dtable_uuid)
            for repo_id, dtable_uuids in repo_id_dtable_uuids_dict.items():
                logger.debug('repo: %s dtable_uuids: %s', repo_id, len(dtable_uuids))
                try:
                    repo = seafile_api.get_repo(repo_id)
                    if not repo:
                        continue
                    for dtable_uuid in dtable_uuids:
                        public_forms_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/public/forms/temp'
                        dir_id = seafile_api.get_dir_id_by_path(repo_id, public_forms_path)
                        if not dir_id:
                            continue
                        f_offset, f_limit= 0, 1000
                        to_delete_files = []
                        while True:
                            dirents = seafile_api.list_dir_by_path(repo_id, public_forms_path, f_offset, f_limit)
                            if not dirents:
                                break
                            for dirent in dirents:
                                if stat.S_ISDIR(dirent.mode):
                                    continue
                                if (now.timestamp() - datetime.fromtimestamp(dirent.mtime)) > self.interval_hours * 60 * 60:
                                    to_delete_files.append(dirent.obj_name)
                            if len(dirents) < f_limit:
                                break
                            f_offset += f_limit
                        logger.debug('repo: %s dtable: %s to delete files: %s', repo_id, dtable_uuid, len(to_delete_files))
                        for file in to_delete_files:
                            seafile_api.del_file(repo_id, public_forms_path, file, '')
                except Exception as e:
                    logger.exception('repo: %s handle upload flags error: %s', repo_id, e)
            if len(results) < limit:
                break
            offset += limit
        sql = "DELETE FROM dtable_form_upload_link_flags WHERE flag_time <= :flag_time"
        try:
            session.execute(text(sql), {'flag_time': flag_time})
            session.commit()
        except Exception as e:
            logger.error('delete upload flags old data flag time: %s error: %s', flag_time, e)

    def run(self):
        logger.info('start to handle upload flags...')
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def handle():
            session = self.session_class()
            try:
                self.handle_flags(session)
            except Exception as e:
                logger.exception('handle upload flags error: %s', e)
            finally:
                session.close()

        sched.start()
