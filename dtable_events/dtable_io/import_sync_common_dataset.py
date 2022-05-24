import json
from copy import deepcopy
from datetime import datetime

import requests
from dtable_events.common_dataset.common_dataset_sync_utils import import_or_sync
from dtable_events.db import init_db_session_class
from dtable_events.dtable_io import dtable_io_logger
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.utils import uuid_str_to_32_chars, get_inner_dtable_server_url


DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
DTABLE_PRIVATE_KEY = task_manager.conf['dtable_private_key']
DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['dtable_proxy_server_url']
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
    dst_headers = context['dst_headers']
    src_table = context['src_table']
    src_view = context['src_view']
    src_columns = context['src_columns']
    src_headers = context['src_headers']

    dst_dtable_uuid = context['dst_dtable_uuid']
    src_dtable_uuid = context['src_dtable_uuid']
    dst_table_id = context['dst_table_id']

    dataset_id = context.get('dataset_id')
    src_version = context.get('src_version')

    # get database version
    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
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
        return
    finally:
        db_session.close()

    if list(sync_dataset):
        return

    # request dst_dtable
    url = dtable_server_url.strip('/') + '/dtables/' + str(dst_dtable_uuid) + '?from=dtable_events'
    try:
        resp = requests.get(url, headers=dst_headers)
        dst_dtable_json = resp.json()
    except Exception as e:
        dtable_io_logger.error('request dst dtable: %s error: %s', dst_dtable_uuid, e)
        return

    # check dst_table
    dst_table = None
    for table in dst_dtable_json.get('tables', []):
        if table.get('_id') == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        dtable_io_logger.error('Destination table: %s not found.' % dst_table_id)
        return
    dst_columns = dst_table.get('columns')
    dst_rows = dst_table.get('rows')

    try:
        result = import_or_sync({
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_dtable_uuid': src_dtable_uuid,
            'src_rows': src_table.get('rows', []),
            'src_columns': src_columns,
            'src_table_name': src_table.get('name'),
            'src_view_name': src_view.get('name'),
            'src_headers': src_headers,
            'dst_table_id': dst_table_id,
            'dst_table_name': dst_table.get('name'),
            'dst_headers': dst_headers,
            'dst_rows': dst_rows,
            'dst_columns': dst_columns
        })
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('sync common dataset error: %s', e)
        raise Exception(str(e))
    else:
        if result and 'task_status_code' in result and result['task_status_code'] != 200:
            dtable_io_logger.error(result)
            error_msg = 'import_sync_common_dataset:%s' % json.dumps(result)
            raise Exception(error_msg)

    # get base's metadata
    src_url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(src_dtable_uuid) + '/metadata/?from=dtable_events'
    try:
        dtable_metadata = requests.get(src_url, headers=src_headers)
        src_metadata = dtable_metadata.json()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('metadata', {}).get('version')

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
    dst_headers = context['dst_headers']
    src_table = context['src_table']
    src_columns = context['src_columns']
    src_view = context['src_view']
    src_headers = context['src_headers']

    dst_dtable_uuid = context['dst_dtable_uuid']
    src_dtable_uuid = context['src_dtable_uuid']
    dst_table_name = context['dst_table_name']
    lang = context.get('lang', 'en')

    dataset_id = context.get('dataset_id')
    creator = context.get('creator')

    try:
        result = import_or_sync({
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_dtable_uuid': src_dtable_uuid,
            'src_rows': src_table.get('rows', []),
            'src_columns': src_columns,
            'src_table_name': src_table.get('name'),
            'src_view_name': src_view.get('name'),
            'src_headers': src_headers,
            'dst_table_name': dst_table_name,
            'dst_headers': dst_headers,
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
    url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(src_dtable_uuid) + '/metadata/?from=dtable_events'
    try:
        dtable_metadata = requests.get(url, headers=src_headers)
        src_metadata = dtable_metadata.json()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('metadata', {}).get('version')

    sql = '''
        INSERT INTO dtable_common_dataset_sync (`dst_dtable_uuid`, `dst_table_id`, `created_at`, `creator`, `last_sync_time`, `dataset_id`, `src_version`)
        VALUES (:dst_dtable_uuid, :dst_table_id, :created_at, :creator, :last_sync_time, :dataset_id, :src_version)
    '''

    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dst_table_id': dst_table_id,
            'created_at': datetime.now(),
            'creator': creator,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id,
            'src_version': last_src_version
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable common dataset sync error: %s', e)
    finally:
        db_session.close()
