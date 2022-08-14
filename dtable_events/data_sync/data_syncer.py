import json
import logging
from datetime import datetime, timedelta
from threading import Thread
from concurrent.futures import ThreadPoolExecutor, ALL_COMPLETED, wait

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, uuid_str_to_36_chars, get_inner_dtable_server_url
from dtable_events.data_sync.data_sync_utils import run_sync_emails, set_data_sync_invalid, sync_email, check_imap_account
from dtable_events.automations.models import get_third_party_account

logger = logging.getLogger(__name__)


class DataSyncer(object):

    def __init__(self, config):
        self._enabled = True
        self._max_workers = 5
        self._prepara_config(config)
        self._db_session_class = init_db_session_class(config)

    def _prepara_config(self, config):
        section_name = 'EMAIL-SYNCER'
        key_enabled = 'enabled'
        key_max_workers = 'max_workers'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        self._enabled = parse_bool(enabled)
        # max workers
        max_workers = get_opt_from_conf_or_env(config, section_name, key_max_workers, default=5)
        try:
            self._max_workers = int(max_workers)
        except:
            pass
        finally:
            self._max_workers = min(32, self._max_workers)

    def start(self):
        if not self.is_enabled():
            logging.warning('Email syncer not enabled')
            return
        DataSyncerTimer(self._db_session_class, self._max_workers).start()

    def is_enabled(self):
        return self._enabled


def list_pending_data_syncs(db_session):
    sql = '''
            SELECT das.id, das.dtable_uuid, das.sync_type, das.detail, w.repo_id, w.id FROM dtable_data_syncs das
            INNER JOIN dtables d ON das.dtable_uuid=d.uuid AND d.deleted=0
            INNER JOIN workspaces w ON w.id=d.workspace_id
            WHERE das.is_valid=1
        '''

    dataset_list = db_session.execute(sql)
    return dataset_list


def run_sync_task(context):
    try:
        if context['sync_type'] == 'email':
            run_sync_emails(context)
    except Exception as e:
        logging


def check_data_syncs(db_session, max_workers):
    data_sync_list = list_pending_data_syncs(db_session)

    executor = ThreadPoolExecutor(max_workers=max_workers)

    sync_infos = []
    for data_sync in data_sync_list:
        sync_infos.append({
            'data_sync_id': data_sync[0],
            'dtable_uuid': uuid_str_to_36_chars(data_sync[1]),
            'sync_type': data_sync[2],
            'detail': json.loads(data_sync[3]),
            'repo_id': data_sync[4],
            'workspace_id': data_sync[5],
            'db_session': db_session
        })

        if len(sync_infos) == max_workers:
            tasks = [executor.submit(run_sync_task, sync_info) for sync_info in sync_infos]
            wait(tasks, return_when=ALL_COMPLETED)
            sync_infos = []
    if sync_infos:
        tasks = [executor.submit(run_sync_task, sync_info) for sync_info in sync_infos]
        wait(tasks, return_when=ALL_COMPLETED)


class DataSyncerTimer(Thread):
    def __init__(self, db_session_class, max_workers=5):
        super(DataSyncerTimer, self).__init__()
        self.db_session_class = db_session_class
        self.max_workers = max_workers

    def run(self):
        sched = BlockingScheduler()
        # fire at the 30th minute of every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='30')
        def timed_job():
            logging.info('Starts to scan data syncs...')
            db_session = self.db_session_class()
            try:
                check_data_syncs(db_session, self.max_workers)
            except Exception as e:
                logger.exception('check periodical data syncs error: %s', e)
            finally:
                db_session.close()

        sched.start()
