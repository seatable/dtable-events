import logging
import time
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread

import jwt
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from dtable_events import init_db_session_class
from dtable_events.app.config import DTABLE_PRIVATE_KEY
from dtable_events.common_dataset.common_dataset_sync_utils import batch_sync_common_dataset
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool

class CommonDatasetSyncer(object):

    def __init__(self, config):
        self._enabled = True
        self._prepara_config(config)
        self._db_session_class = init_db_session_class(config)

    def _prepara_config(self, config):
        section_name = 'COMMON-DATASET-SYNCER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        self._enabled = parse_bool(enabled)

    def start(self):
        if not self.is_enabled():
            logging.warning('Common dataset syncer not enabled')
            return
        CommonDatasetSyncerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def get_dtable_server_header(dtable_uuid):
    try:
        access_token = jwt.encode({
            'dtable_uuid': dtable_uuid,
            'username': 'dtable-events',
            'permission': 'rw',
            'exp': int(time.time()) + 60
        },
            DTABLE_PRIVATE_KEY,
            algorithm='HS256'
        )
    except Exception as e:
        logging.error(e)
        return
    return {'Authorization': 'Token ' + access_token}


def list_pending_common_dataset_syncs(db_session):
    sql = '''
            SELECT dcds.dst_dtable_uuid, dcds.dst_table_id, dcd.table_id AS src_table_id, dcd.view_id AS src_view_id,
                dcd.dtable_uuid AS src_dtable_uuid, dcds.id AS sync_id, dcds.src_version, dcd.id AS dataset_id
            FROM dtable_common_dataset dcd
            INNER JOIN dtable_common_dataset_sync dcds ON dcds.dataset_id=dcd.id
            INNER JOIN dtables d_src ON dcd.dtable_uuid=d_src.uuid AND d_src.deleted=0
            INNER JOIN dtables d_dst ON dcds.dst_dtable_uuid=d_dst.uuid AND d_dst.deleted=0
            WHERE dcds.is_sync_periodically=1 AND dcd.is_valid=1 AND dcds.is_valid=1 AND 
            ((dcds.sync_interval='per_day' AND dcds.last_sync_time<:per_day_check_time) OR 
            (dcds.sync_interval='per_hour'))
        '''

    per_day_check_time = datetime.now() - timedelta(hours=23)
    dataset_list = db_session.execute(text(sql), {
        'per_day_check_time': per_day_check_time
    })
    return dataset_list


def check_common_dataset(session_class):
    with session_class() as db_session:
        dataset_sync_list = list(list_pending_common_dataset_syncs(db_session))
        cds_dst_dict = defaultdict(list)
        for dataset_sync in dataset_sync_list:
            cds_dst_dict[dataset_sync.dataset_id].append(dataset_sync)
        for dataset_id, dataset_syncs in cds_dst_dict.items():
            batch_sync_common_dataset(dataset_id, dataset_syncs, db_session)


class CommonDatasetSyncerTimer(Thread):
    def __init__(self, db_session_class):
        super(CommonDatasetSyncerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan common dataset syncs...')
            try:
                check_common_dataset(self.db_session_class)
            except Exception as e:
                logging.exception('check periodcal common dataset syncs error: %s', e)

        sched.start()
