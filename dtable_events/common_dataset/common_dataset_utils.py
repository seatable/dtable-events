# -*- coding: utf-8 -*-
import logging
import time
import json
import os
import uuid
from datetime import datetime, timedelta
import requests
import jwt
import sys

logger = logging.getLogger(__name__)


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist.')

sys.path.insert(0, dtable_web_dir)

try:
    from seahub.settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL, \
        ENABLE_DTABLE_SERVER_CLUSTER, DTABLE_PROXY_SERVER_URL
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)


def list_synchronizing_common_dataset(db_session):
    sql = '''
            SELECT b.dst_dtable_uuid,b.dst_table_id,a.table_id as src_table_id,a.view_id as src_view_id,
                a.dataset_name,a.dtable_uuid as src_dtable_uuid, b.id as sync_id
            FROM dtable_common_dataset a 
            INNER JOIN dtable_common_dataset_sync b ON b.dataset_id=a.id
            INNER JOIN dtables c ON a.dtable_uuid=c.uuid and c.deleted=0
            INNER JOIN dtables d ON b.dst_dtable_uuid=d.uuid AND d.deleted=0
            WHERE last_sync_time<:per_day_check_time
        '''

    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    dataset_list = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
    })
    return dataset_list


def get_dtable_server_token(dtable_uuid):
    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': 'dtable-web',
        'permission': 'r',
    }
    try:
        access_token = jwt.encode(
            payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
        )
    except Exception as e:
        logger.error(e)
        return
    return access_token


def uuid_str_to_36_chars(dtable_uuid):
    if len(dtable_uuid) == 32:
        return str(uuid.UUID(dtable_uuid))
    else:
        return dtable_uuid


def get_table_dict(dtable_uuid):
    dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    access_token = get_dtable_server_token(dtable_uuid)
    url = api_url.rstrip('/') + '/dtables/' + str(dtable_uuid)
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}
    query_param = {
        'lang': 'en',
    }
    try:
        res = requests.get(url, headers=headers, params=query_param)
    except requests.HTTPError as e:
        logger.error(e)
        return {}

    res = json.loads(res.content)
    tables = res['tables']
    table_id_dict = {table['_id']: [view['_id'] for view in table['views']] for table in tables}
    return table_id_dict


def sync_common_dataset(dst_dtable_uuid, dst_table_id, table_id, view_id, dataset_name, dtable_uuid):
    dst_dtable_uuid = uuid_str_to_36_chars(dst_dtable_uuid)
    dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    access_token = get_dtable_server_token(dst_dtable_uuid)
    url = api_url.rstrip('/') + '/api/v1/dtables/' + str(
        dst_dtable_uuid) + '/sync-common-dataset/?from=dtable_web'
    headers = {'Authorization': 'Token ' + access_token.decode('utf-8')}

    data = {
        'table_id': table_id,
        'view_id': view_id,
        'table_name': dataset_name,
        'dst_table_id': dst_table_id,
        'src_dtable_uuid': str(dtable_uuid),
    }
    try:
        res = requests.post(url, headers=headers, data=data)
    except requests.HTTPError as e:
        logger.error(e)
        return
    if res.status_code != 200:
        logger.error(f'failed to sync common dataset {res.text}')


def update_sync_time(db_session, update_list):
    sql = '''
        UPDATE dtable_common_dataset_sync SET last_sync_time=NOW() WHERE id IN :update_list
        '''
    db_session.execute(sql, {'update_list': update_list})
    db_session.commit()


def check_common_dataset(db_session):
    sync_dataset_list = list_synchronizing_common_dataset(db_session)
    sync_id_list = []
    for dataset in sync_dataset_list:
        dst_dtable_uuid = dataset[0]
        dst_table_id = dataset[1]
        src_table_id = dataset[2]
        src_view_id = dataset[3]
        dataset_name = dataset[4]
        src_dtable_uuid = dataset[5]
        sync_id = dataset[6]

        dst_table_dict = get_table_dict(uuid_str_to_36_chars(dst_dtable_uuid))
        if dst_table_id not in dst_table_dict:
            # if dst_table_id not found  continue
            continue
        src_table_dict = get_table_dict(uuid_str_to_36_chars(src_dtable_uuid))
        if src_table_id not in src_table_dict or src_view_id not in src_table_dict[src_table_id]:
            # if src_table_id not found  continue, if src_view not found  continue
            continue

        sync_id_list.append(sync_id)
        try:
            sync_common_dataset(dst_dtable_uuid, dst_table_id, src_table_id, src_view_id, dataset_name, src_dtable_uuid)
        except Exception as e:
            logging.error(f'sync common dataset failed. {dataset}, error: {e}')
    if sync_id_list:
        try:
            update_sync_time(db_session, sync_id_list)
        except Exception as e:
            logging.error(f'update last sync time failed, error: {e}')
