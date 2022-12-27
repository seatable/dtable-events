import json
from copy import deepcopy
from datetime import datetime

import requests
from dtable_events.common_dataset.common_dataset_sync_utils import import_sync_CDS
from dtable_events.db import init_db_session_class
from dtable_events.dtable_io import dtable_io_logger
from dtable_events.utils import uuid_str_to_32_chars, get_inner_dtable_server_url
from dtable_events.utils.dtable_server_api import DTableServerAPI

dtable_server_url = get_inner_dtable_server_url()


def sync_common_dataset(context, config):
    """
    sync common dataset to destination table

    :param dst_dtable: destination dtable
    :param src_dtable: source dtable
    :param table_id: source table id
    :param view_id: source view id
    :param dst_table_id: destination table id

    :return api_error or None
    """
    src_dtable_uuid = context.get('src_dtable_uuid')
    dst_dtable_uuid = context.get('dst_dtable_uuid')

    src_table_name = context.get('src_table_name')
    src_view_name = context.get('src_view_name')
    src_columns = context.get('src_columns')
    src_version = context.get('src_version')

    dst_table_id = context.get('dst_table_id')
    dst_table_name = context.get('dst_table_name')
    dst_columns = context.get('dst_columns')

    operator = context.get('operator')
    lang = context.get('lang', 'en')

    dataset_id = context.get('dataset_id')

    # get database version
    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        return

    sql = '''
                SELECT id FROM dtable_common_dataset_sync 
                WHERE dst_dtable_uuid=:dst_dtable_uuid AND dataset_id=:dataset_id AND dst_table_id=:dst_table_id 
                AND src_version=:src_version
            '''
    try:
        sync_dataset = db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dataset_id': dataset_id,
            'dst_table_id': dst_table_id,
            'src_version': src_version
        })
    except Exception as e:
        dtable_io_logger.error('get src version error: %s', e)
        db_session.close()
        return

    sync_dataset = list(sync_dataset)
    if sync_dataset:
        dtable_io_logger.debug('sync_dataset: %s', sync_dataset[0])
        return

    try:
        result = import_sync_CDS({
            'src_dtable_uuid': src_dtable_uuid,
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_table_name': src_table_name,
            'src_view_name': src_view_name,
            'src_columns': src_columns,
            'dst_table_id': dst_table_id,
            'dst_table_name': dst_table_name,
            'dst_columns': dst_columns,
            'operator': operator,
            'lang': lang
        })
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('sync common dataset error: %s', e)
        db_session.close()
        raise Exception(str(e))
    else:
        if result and 'task_status_code' in result and result['task_status_code'] != 200:
            if result['task_status_code'] == 500:
                dtable_io_logger.error(result)
            error_msg = 'import_sync_common_dataset:%s' % json.dumps(result)
            raise Exception(error_msg)

    # get base's metadata
    src_dtable_server_api = DTableServerAPI(operator, src_dtable_uuid, dtable_server_url)
    try:
        src_metadata = src_dtable_server_api.get_metadata()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('version')

    sql = '''
        UPDATE dtable_common_dataset_sync SET
        last_sync_time=:last_sync_time, src_version=:last_src_version
        WHERE dataset_id=:dataset_id AND dst_dtable_uuid=:dst_dtable_uuid AND dst_table_id=:dst_table_id
    '''
    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dst_table_id': dst_table_id,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id,
            'last_src_version': last_src_version
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable common dataset sync error: %s', e)
    finally:
        db_session.close()


def import_common_dataset(context, config):
    """
    import common dataset to destination table
    """
    src_dtable_uuid = context.get('src_dtable_uuid')
    dst_dtable_uuid = context.get('dst_dtable_uuid')

    src_table_name = context.get('src_table_name')
    src_view_name = context.get('src_view_name')
    src_columns = context.get('src_columns')

    dst_table_name = context.get('dst_table_name')

    operator = context.get('operator')
    lang = context.get('lang', 'en')

    dataset_id = context.get('dataset_id')

    try:
        result = import_sync_CDS({
            'src_dtable_uuid': src_dtable_uuid,
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_table_name': src_table_name,
            'src_view_name': src_view_name,
            'src_columns': src_columns,
            'dst_table_name': dst_table_name,
            'operator': operator,
            'lang': lang
        })
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('import common dataset error: %s', e)
        raise Exception(e)
    else:
        if result and 'task_status_code' in result and result['task_status_code'] != 200:
            dtable_io_logger.error(result['error_msg'])
            error_msg = 'import_sync_common_dataset:%s' % json.dumps(result)
            raise Exception(error_msg)
        dst_table_id = result.get('dst_table_id')

    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        return

    # get base's metadata
    src_dtable_server_api = DTableServerAPI(operator, src_dtable_uuid, dtable_server_url)
    try:
        src_metadata = src_dtable_server_api.get_metadata()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('version')

    sql = '''
        INSERT INTO dtable_common_dataset_sync (`dst_dtable_uuid`, `dst_table_id`, `created_at`, `creator`, `last_sync_time`, `dataset_id`, `src_version`)
        VALUES (:dst_dtable_uuid, :dst_table_id, :created_at, :creator, :last_sync_time, :dataset_id, :src_version)
    '''

    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dst_table_id': dst_table_id,
            'created_at': datetime.now(),
            'creator': operator,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id,
            'src_version': last_src_version
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable common dataset sync error: %s', e)
    finally:
        db_session.close()
