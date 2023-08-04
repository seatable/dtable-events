import logging
import time
from datetime import datetime, timedelta
from threading import Thread

import jwt
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events import init_db_session_class
from dtable_events.app.config import DTABLE_PRIVATE_KEY
from dtable_events.common_dataset.common_dataset_sync_utils import import_sync_CDS, set_common_dataset_sync_invalid
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


def gen_src_dst_assets(dst_dtable_uuid, src_dtable_uuid, src_table_id, src_view_id, dst_table_id, dataset_sync_id, dataset_id, db_session):
    """
    return assets -> dict
    """
    dtable_server_url = get_inner_dtable_server_url()
    src_dtable_server_api = DTableServerAPI('dtable-events', src_dtable_uuid, dtable_server_url)
    dst_dtable_server_api = DTableServerAPI('dtable-events', dst_dtable_uuid, dtable_server_url)
    try:
        src_dtable_metadata = src_dtable_server_api.get_metadata()
        dst_dtable_metadata = dst_dtable_server_api.get_metadata()
    except Exception as e:
        logging.error('request src dst dtable: %s, %s metadata error: %s', src_dtable_uuid, dst_dtable_uuid, e)
        return None

    src_table, src_view = None, None
    for table in src_dtable_metadata.get('tables', []):
        if table['_id'] == src_table_id:
            src_table = table
            break
    if not src_table:
        set_common_dataset_sync_invalid(dataset_sync_id, db_session)
        logging.error('Source table not found.')
        return None
    for view in src_table.get('views', []):
        if view['_id'] == src_view_id:
            src_view = view
            break
    if not src_view:
        set_common_dataset_sync_invalid(dataset_sync_id, db_session)
        logging.error('Source view not found.')
        return None

    src_columns = [col for col in src_table.get('columns', []) if col['key'] not in src_view.get('hidden_columns', [])]

    src_enable_archive = (src_dtable_metadata.get('settings') or {}).get('enable_archive', False)
    src_version = src_dtable_metadata.get('version')

    dst_table = None
    if dst_table_id:
        for table in dst_dtable_metadata.get('tables', []):
            if table['_id'] == dst_table_id:
                dst_table = table
                break
        if not dst_table:
            set_common_dataset_sync_invalid(dataset_sync_id, db_session)
            logging.error('Destination table not found.')
            return None

    return {
        'src_table_name': src_table['name'],
        'src_view_name': src_view['name'],
        'src_columns': src_columns,
        'src_version': src_version,
        'dst_table_name': dst_table['name'] if dst_table else None,
        'dst_columns': dst_table['columns'] if dst_table else None
    }


def list_pending_common_dataset_syncs(db_session):
    sql = '''
            SELECT dcds.dst_dtable_uuid, dcds.dst_table_id, dcd.table_id AS src_table_id, dcd.view_id AS src_view_id,
                dcd.dtable_uuid AS src_dtable_uuid, dcds.id AS sync_id, dcds.src_version, dcd.id
            FROM dtable_common_dataset dcd
            INNER JOIN dtable_common_dataset_sync dcds ON dcds.dataset_id=dcd.id
            INNER JOIN dtables d_src ON dcd.dtable_uuid=d_src.uuid AND d_src.deleted=0
            INNER JOIN dtables d_dst ON dcds.dst_dtable_uuid=d_dst.uuid AND d_dst.deleted=0
            WHERE dcds.is_sync_periodically=1 AND dcd.is_valid=1 AND dcds.is_valid=1 AND 
            ((dcds.sync_interval='per_day' AND dcds.last_sync_time<:per_day_check_time) OR 
            (dcds.sync_interval='per_hour'))
        '''

    per_day_check_time = datetime.now() - timedelta(hours=23)
    dataset_list = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time
    })
    return dataset_list


def update_sync_time_and_version(db_session, update_map):
    """
    :param update_map: {dataset_sync_id:src_version,dataset_sync_id1:src_version1}
    """
    src_version_sql = ""
    for dataset_sync_id, src_version in update_map.items():
        sql_str = " WHEN " + str(dataset_sync_id) + " THEN " + str(src_version)
        src_version_sql += sql_str

    sql = "UPDATE dtable_common_dataset_sync SET last_sync_time=:last_sync_time, src_version=CASE id" \
          + src_version_sql + " END WHERE id IN :dataset_sync_id_list"

    dataset_sync_id_list = [dataset_sync_id for dataset_sync_id in update_map]
    db_session.execute(sql, {'dataset_sync_id_list': dataset_sync_id_list, 'last_sync_time': datetime.now()})
    db_session.commit()


def check_common_dataset(db_session):
    dataset_sync_list = list_pending_common_dataset_syncs(db_session)
    sync_count = 0
    dataset_update_map = {}
    for dataset_sync in dataset_sync_list:
        dst_dtable_uuid = uuid_str_to_36_chars(dataset_sync[0])
        dst_table_id = dataset_sync[1]
        src_table_id = dataset_sync[2]
        src_view_id = dataset_sync[3]
        src_dtable_uuid = uuid_str_to_36_chars(dataset_sync[4])
        dataset_sync_id = dataset_sync[5]
        last_src_version = dataset_sync[6]
        dataset_id = dataset_sync[7]

        assets = gen_src_dst_assets(dst_dtable_uuid, src_dtable_uuid, src_table_id, src_view_id, dst_table_id, dataset_sync_id, dataset_id, db_session)

        if not assets:
            continue

        if assets.get('src_version') == last_src_version:
            continue

        src_table_name = assets.get('src_table_name')
        src_view_name = assets.get('src_view_name')
        dst_table_name = assets.get('dst_table_name')
        try:
            result = import_sync_CDS({
                'src_dtable_uuid': src_dtable_uuid,
                'dst_dtable_uuid': dst_dtable_uuid,
                'src_table_name': src_table_name,
                'src_view_name': src_view_name,
                'src_columns': assets.get('src_columns'),
                'dst_table_id': dst_table_id,
                'dst_table_name': dst_table_name,
                'dst_columns': assets.get('dst_columns'),
                'operator': 'dtable-events',
                'lang': 'en',  # TODO: lang
            })
        except Exception as e:
            logging.error('sync common dataset src-uuid: %s src-table: %s src-view: %s dst-uuid: %s dst-table: %s error: %s', 
                          src_dtable_uuid, src_table_name, src_view_name, dst_dtable_uuid, dst_table_name, e)
            continue
        else:
            if result.get('error_msg'):
                logging.error(result['error_msg'])
                if result.get('error_type') == 'generate_synced_columns_error':
                    logging.warning('src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s generate sync-columns error: %s',
                                    src_dtable_uuid, src_table_id, src_view_id, dst_dtable_uuid, dst_table_id, result)

        dataset_update_map[dataset_sync_id] = assets.get('src_version')
        sync_count += 1

        if sync_count == 1000:
            try:
                update_sync_time_and_version(db_session, dataset_update_map)
            except Exception as e:
                logging.error(f'update sync time and src_version failed, error: {e}')
            dataset_update_map = {}
            sync_count = 0

    if dataset_update_map:
        try:
            update_sync_time_and_version(db_session, dataset_update_map)
        except Exception as e:
            logging.error(f'update sync time and src_version failed, error: {e}')


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
            db_session = self.db_session_class()
            try:
                check_common_dataset(db_session)
            except Exception as e:
                logging.exception('check periodcal common dataset syncs error: %s', e)
            finally:
                db_session.close()

        sched.start()
