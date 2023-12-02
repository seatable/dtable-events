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
from dtable_events.common_dataset.common_dataset_sync_utils import import_sync_CDS, set_common_dataset_syncs_invalid, get_dataset_data
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, uuid_str_to_36_chars, get_inner_dtable_server_url
from dtable_events.utils.dtable_server_api import DTableServerAPI

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


def gen_src_assets(src_dtable_uuid, src_table_id, src_view_id, dataset_sync_ids, db_session):
    """
    :return: assets -> dict
    """
    dtable_server_url = get_inner_dtable_server_url()
    src_dtable_server_api = DTableServerAPI('dtable-events', src_dtable_uuid, dtable_server_url)
    try:
        src_dtable_metadata = src_dtable_server_api.get_metadata()
    except Exception as e:
        logging.error('request src dst dtable: %s metadata error: %s', src_dtable_uuid, e)
        return None
    src_table, src_view = None, None
    for table in src_dtable_metadata.get('tables', []):
        if table['_id'] == src_table_id:
            src_table = table
            break
    if not src_table:
        set_common_dataset_syncs_invalid(dataset_sync_ids, db_session)
        logging.warning('src: %s, %s, %s Source table not found.', src_dtable_uuid, src_table_id, src_view_id)
        return None
    for view in src_table.get('views', []):
        if view['_id'] == src_view_id:
            src_view = view
            break
    if not src_view:
        set_common_dataset_syncs_invalid(dataset_sync_ids, db_session)
        logging.warning('src: %s, %s, %s Source view not found.', src_dtable_uuid, src_table_id, src_view_id)
        return None

    src_version = src_dtable_metadata.get('version')

    return {
        'src_table': src_table,
        'src_version': src_version
    }


def gen_dst_assets(dst_dtable_uuid, dst_table_id, dataset_sync_id, db_session):
    """
    :return: assets -> dict
    """
    dtable_server_url = get_inner_dtable_server_url()
    dst_dtable_server_api = DTableServerAPI('dtable-events', dst_dtable_uuid, dtable_server_url)
    try:
        dst_dtable_metadata = dst_dtable_server_api.get_metadata()
    except Exception as e:
        logging.error('request src dst dtable: %s metadata error: %s', dst_dtable_uuid, e)
        return None
    dst_table = None
    for table in dst_dtable_metadata.get('tables', []):
        if table['_id'] == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        set_common_dataset_syncs_invalid([dataset_sync_id], db_session)
        logging.warning('sync: %s destination table not found.', dataset_sync_id)
        return None
    return {
        'dst_table_name': dst_table['name'],
        'dst_columns': dst_table['columns']
    }


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
        # fetch src assets
        src_dtable_uuid = uuid_str_to_36_chars(dataset_syncs[0].src_dtable_uuid)
        src_table_id = dataset_syncs[0].src_table_id
        src_view_id = dataset_syncs[0].src_view_id
        sync_ids = [dataset_sync.sync_id for dataset_sync in dataset_syncs]
        src_assets = gen_src_assets(src_dtable_uuid, src_table_id, src_view_id, sync_ids, db_session)
        if not src_assets:
            continue
        src_table = src_assets.get('src_table')
        try:
            datase_data, error = get_dataset_data(src_dtable_uuid, src_table, src_view_id)
        except Exception as e:
            logging.exception('request dtable: %s table: %s view: %s data error: %s', src_dtable_uuid, src_table_id, src_view_id, e)
        if error:
            logging.error('request dtable: %s table: %s view: %s data error: %s', src_dtable_uuid, src_table_id, src_view_id, error)
            continue
        for dataset_sync in dataset_syncs:
            dst_dtable_uuid = uuid_str_to_36_chars(dataset_sync.dst_dtable_uuid)
            dst_table_id = dataset_sync.dst_table_id
            dataset_sync_id = dataset_sync.sync_id
            last_src_version = dataset_sync.src_version

            dst_assets = gen_dst_assets(dst_dtable_uuid, dst_table_id, dataset_sync_id, db_session)

            if not dst_assets:
                continue

            if src_assets.get('src_version') == last_src_version:
                continue

            src_table = src_assets.get('src_table')
            dst_table_name = dst_assets.get('dst_table_name')
            try:
                result = import_sync_CDS({
                    'dataset_id': dataset_id,
                    'src_dtable_uuid': src_dtable_uuid,
                    'dst_dtable_uuid': dst_dtable_uuid,
                    'src_table': src_table,
                    'src_view_id': src_view_id,
                    'dst_table_id': dst_table_id,
                    'dst_table_name': dst_table_name,
                    'dst_columns': dst_assets.get('dst_columns'),
                    'operator': 'dtable-events',
                    'lang': 'en',  # TODO: lang
                    'dataset_data': datase_data
                })
            except Exception as e:
                logging.error('sync common dataset src-uuid: %s src-table: %s src-view: %s dst-uuid: %s dst-table: %s error: %s', 
                            src_dtable_uuid, src_table['name'], src_view_id, dst_dtable_uuid, dst_table_name, e)
                continue
            else:
                if result.get('error_msg'):
                    if result.get('error_type') in (
                        'generate_synced_columns_error',
                        'base_exceeds_limit',
                        'exceed_columns_limit',
                        'exceed_rows_limit'
                    ):
                        logging.warning('src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s client error: %s',
                                        src_dtable_uuid, src_table_id, src_view_id, dst_dtable_uuid, dst_table_id, result)
                        with session_class() as db_session:
                            set_common_dataset_syncs_invalid([dataset_sync_id], db_session)
                    else:
                        logging.error('src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s error: %s',
                                    src_dtable_uuid, src_table_id, src_view_id, dst_dtable_uuid, dst_table_id, result)
                    continue
            sql = '''
                UPDATE dtable_common_dataset_sync SET last_sync_time=:last_sync_time, src_version=:src_version
                WHERE id=:id
            '''
            with session_class() as db_session:
                db_session.execute(text(sql), {
                    'last_sync_time': datetime.now(),
                    'src_version': src_assets.get('src_version'),
                    'id': dataset_sync_id
                })
                db_session.commit()


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
