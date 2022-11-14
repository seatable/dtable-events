# -*- coding: utf-8 -*-
import os
import re
import time
import json
from urllib.parse import unquote

import jwt
import requests

from seaserv import seafile_api

from dtable_events.app.config import DTABLE_PRIVATE_KEY, DTABLE_WEB_SERVICE_URL
from dtable_events.dtable_io import dtable_io_logger
from dtable_events.utils import get_inner_dtable_server_url

service_url = DTABLE_WEB_SERVICE_URL.strip()
dtable_server_url = get_inner_dtable_server_url().rstrip('/')


def _trans_url(url, workspace_id, dtable_uuid):
    if url.startswith(service_url):
        return re.sub(r'\d+/asset/[-\w]{36}', workspace_id + '/asset/' + dtable_uuid, url)
    return url


def _trans_file_url(file, workspace_id, dtable_uuid):
    file['url'] = _trans_url(file['url'], workspace_id, dtable_uuid)
    return file


def _trans_image_url(image_url, workspace_id, dtable_uuid):
    return _trans_url(image_url, workspace_id, dtable_uuid)


def _trans_long_text(long_text, workspace_id, dtable_uuid):
    new_images = []
    for image_url in long_text['images']:
        new_image_url = _trans_url(image_url, workspace_id, dtable_uuid)
        long_text['text'] = long_text['text'].replace(image_url, new_image_url)
        new_images.append(new_image_url)
    long_text['images'] = new_images
    return long_text


def _parse_asset_path(url):
    asset_path = ''
    if url.startswith(service_url):
        url = unquote(url)
        asset_path = url[url.find('/asset/')+44:]
    return asset_path


def _trans_rows_content(dst_workspace_id, dst_dtable_uuid, row, img_cols, file_cols, long_text_cols):
    for img_col in img_cols:
        if img_col in row and isinstance(row[img_col], list):
            row[img_col] = [_trans_image_url(img, dst_workspace_id, dst_dtable_uuid) for img in row.get(img_col, [])]
    for file_col in file_cols:
        if file_col in row and isinstance(row[file_col], list):
            row[file_col] = [_trans_file_url(f, dst_workspace_id, dst_dtable_uuid) for f in row.get(file_col, [])]
    for long_text_col in long_text_cols:
        if row.get(long_text_col) and isinstance(row[long_text_col], dict) \
                and row[long_text_col].get('text') and row[long_text_col].get('images'):
            row[long_text_col] = _trans_long_text(row[long_text_col], dst_workspace_id, dst_dtable_uuid)


def _get_asset_path_list(row, img_cols, file_cols, long_text_cols):
    asset_path_list = set()
    for img_col in img_cols:
        if img_col in row and isinstance(row[img_col], list):
            [asset_path_list.add(_parse_asset_path(img)) for img in row.get(img_col, [])]
    for file_col in file_cols:
        if file_col in row and isinstance(row[file_col], list):
            [asset_path_list.add(_parse_asset_path(f['url'])) for f in row.get(file_col, [])]
    for long_text_col in long_text_cols:
        if row.get(long_text_col) and isinstance(row[long_text_col], dict) \
                and row[long_text_col].get('text') and row[long_text_col].get('images'):
            [asset_path_list.add(_parse_asset_path(image_url)) for image_url in row[long_text_col]['images']]

    return asset_path_list


def _copy_table_assets(asset_path_list, src_repo_id, src_dtable_uuid, dst_repo_id, dst_dtable_uuid, username):
    src_asset_dir = os.path.join('/asset', src_dtable_uuid)
    src_asset_dir_id = seafile_api.get_dir_id_by_path(src_repo_id, src_asset_dir)
    if src_asset_dir_id:
        dst_asset_dir = os.path.join('/asset', dst_dtable_uuid)
        if not seafile_api.get_dir_id_by_path(dst_repo_id, dst_asset_dir):
            seafile_api.mkdir_with_parents(dst_repo_id, '/', dst_asset_dir[1:], username)
        for asset_path in list(asset_path_list):
            src_full_path = os.path.dirname(os.path.join('/asset', src_dtable_uuid, asset_path))
            dst_full_path = os.path.dirname(os.path.join('/asset', dst_dtable_uuid, asset_path))
            if not seafile_api.get_dir_id_by_path(dst_repo_id, dst_full_path):
                seafile_api.mkdir_with_parents(dst_repo_id, '/', dst_full_path[1:], username)
            file_name = os.path.basename(asset_path)
            seafile_api.copy_file(src_repo_id, src_full_path, json.dumps([file_name]),
                                  dst_repo_id, dst_full_path, json.dumps([file_name]),
                                  username, need_progress=1)


def trans_and_copy_asset(table, src_repo_id, src_dtable_uuid, dst_workspace_id, dst_repo_id, dst_dtable_uuid, username):
    try:
        img_cols = [col['key'] for col in table['columns'] if col['type'] == 'image']
        file_cols = [col['key'] for col in table['columns'] if col['type'] == 'file']
        long_text_cols = [col['key'] for col in table['columns'] if col['type'] == 'long-text']

        asset_path_list = set()
        for row in table['rows']:
            _trans_rows_content(dst_workspace_id, dst_dtable_uuid, row, img_cols, file_cols, long_text_cols)
            asset_path_list = asset_path_list | _get_asset_path_list(row, img_cols, file_cols, long_text_cols)

        _copy_table_assets(list(asset_path_list), src_repo_id, src_dtable_uuid,
                           dst_repo_id, dst_dtable_uuid, username)
    except Exception as e:
        dtable_io_logger.error('trans_and_copy_asset: %s' % e)
        return False, None
    return True, table


def import_table_from_base(context):
    """import table from base
    """
    # extract params
    username = context['username']
    src_repo_id = context['src_repo_id']
    src_dtable_uuid = context['src_dtable_uuid']
    src_table_id = context['src_table_id']
    dst_workspace_id = context['dst_workspace_id']
    dst_repo_id = context['dst_repo_id']
    dst_dtable_uuid = context['dst_dtable_uuid']
    dst_table_name = context['dst_table_name']
    lang = context.get('lang', 'en')

    try:
        # generate src_headers
        src_payload = {
            'dtable_uuid': src_dtable_uuid,
            'username': username,
            'permission': 'r',
            'exp': int(time.time()) + 60
        }
        src_access_token = jwt.encode(src_payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
        src_headers = {'Authorization': 'Token ' + src_access_token}

        # get src_base's data
        url = '%s/dtables/%s/?from=dtable_events' % (dtable_server_url, src_dtable_uuid)
        resp = requests.get(url, headers=src_headers, timeout=180)
        src_dtable_json = resp.json()

        # get src_table and src_columns
        src_table = None
        for table in src_dtable_json.get('tables', []):
            if table.get('_id') == src_table_id:
                src_table = table
                break

        if not src_table:
            error_msg = 'Table %s not found.' % src_table_id
            dtable_io_logger.error(error_msg)
            raise Exception(error_msg)

        src_columns = src_table.get('columns', [])

        # trans asset url and copy asset
        succeed, new_table = trans_and_copy_asset(
            src_table, src_repo_id, src_dtable_uuid, dst_workspace_id, dst_repo_id, dst_dtable_uuid, username)
        if not succeed:
            error_msg = 'trans asset url and copy asset error'
            dtable_io_logger.error(error_msg)
            raise Exception(error_msg)

        # generate dst_headers
        dst_payload = {
            'dtable_uuid': dst_dtable_uuid,
            'username': username,
            'permission': 'rw',
            'exp': int(time.time()) + 60*5
        }
        dst_access_token = jwt.encode(dst_payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
        dst_headers = {'Authorization': 'Token ' + dst_access_token}

        # create dst_table
        url = '%s/api/v1/dtables/%s/tables/?from=dtable_events' % (dtable_server_url, dst_dtable_uuid)
        dst_columns = [{
                'column_key': col.get('key'),
                'column_name': col.get('name'),
                'column_type': col.get('type'),
                'column_data': col.get('data')
            } for col in src_columns] if src_columns else []
        data = {
            'lang': lang,
            'table_name': dst_table_name,
            'columns': dst_columns,
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data, timeout=180)
            if resp.status_code != 200:
                error_msg = 'create dst table error, status code: %s, resp text: %s' \
                            % (resp.status_code, resp.text)
                dtable_io_logger.error(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            error_msg = 'create dst table error: %s' % e
            dtable_io_logger.error(error_msg)
            raise Exception(error_msg)

        # import src_rows step by step
        src_rows = new_table.get('rows', [])
        step = 1000
        url = '%s/api/v1/dtables/%s/batch-append-rows/?from=dtable_events' % (dtable_server_url, dst_dtable_uuid)
        for i in range(0, len(src_rows), step):
            data = {
                'table_name': dst_table_name,
                'rows': src_rows[i: i + step],
                'need_convert_back': False
            }
            try:
                resp = requests.post(url, headers=dst_headers, json=data, timeout=180)
                if resp.status_code != 200:
                    error_msg = 'batch append rows to dst dtable: %s dst table: %s error: %s status_code: %s' % \
                                (dst_dtable_uuid, dst_table_name, resp.text, resp.status_code)
                    dtable_io_logger.error(error_msg)
                    raise Exception(error_msg)
            except Exception as e:
                error_msg = 'batch append rows to dst dtable: %s dst table: %s error: %s' % \
                            (dst_dtable_uuid, dst_table_name, e)
                dtable_io_logger.error(error_msg)
                raise Exception(error_msg)
    except Exception as e:
        error_msg = 'import_table_from_base: %s' % e
        dtable_io_logger.error(error_msg)
        raise Exception(error_msg)
