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
from dtable_events.utils.constants import ColumnTypes, DATE_FORMATS, DURATION_FORMATS, NUMBER_FORMATS, NUMBER_DECIMALS,\
    NUMBER_THOUSANDS, GEO_FORMATS
from dtable_events.utils.dtable_column_utils import AutoNumberUtils

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

def trans_page_content_url(content_url, workspace_id, dtable_uuid):
    return _trans_url(content_url, workspace_id, dtable_uuid)


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


def generate_column(src_column):
    column_name = src_column.get('name')
    column_key = src_column.get('key')
    column_type = src_column.get('type')
    column_data = src_column.get('data') or {}
    if not column_name or not column_key or not column_type:
        return None
    column = {
        'column_key': column_key,
        'column_name': column_name,
        'column_type': column_type
    }
    if column_type == ColumnTypes.DATE:
        format = column_data.get('format')
        if not format or format not in DATE_FORMATS:
            column_data['format'] = DATE_FORMATS[0]
        column['column_data'] = column_data
    elif column_type == ColumnTypes.DURATION:
        duration_format = column_data.get('duration_format')
        format = column_data.get('format') or 'duration'
        if not duration_format or duration_format not in DURATION_FORMATS:
            column_data['duration_format'] = DURATION_FORMATS[0]
        column_data['format'] = format
        column['column_data'] = column_data
    elif column_type == ColumnTypes.NUMBER:
        format = column_data.get('format')
        decimal = column_data.get('decimal')
        thousands = column_data.get('thousands')
        if not format or format not in NUMBER_FORMATS:
            column_data['format'] = NUMBER_FORMATS[0]
        if not decimal or decimal not in NUMBER_DECIMALS:
            column_data['decimal'] = NUMBER_DECIMALS[0]
        if not thousands or thousands not in NUMBER_THOUSANDS:
            column_data['thousands'] = NUMBER_THOUSANDS[0]
        column['column_data'] = column_data
    elif column_type in [ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT]:
        old_options = column_data.get('options') or []
        options = []
        for option in old_options:
            if not isinstance(option, dict):
                continue
            if not option.get('id'):
                continue
            if not option.get('name'):
                continue
            if not option.get('color'):
                continue
            options.append(option)
        column_data['options'] = options
        column['column_data'] = column_data
    elif column_type == ColumnTypes.GEOLOCATION:
        geo_format = column_data.get('geo_format')
        if not geo_format or geo_format not in GEO_FORMATS:
            column_data['geo_format'] = GEO_FORMATS[0]
        column['column_data'] = column_data
    elif column_type == ColumnTypes.AUTO_NUMBER:
        format = column_data.get('format')
        if not format:
            column_data['format'] = '0000'
        else:
            try:
                AutoNumberUtils.get_parsed_format(format)
            except:
                column_data['format'] = '0000'
    else:
        column['column_data'] = column_data
    return column


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
        # These column types refer to the data of other columns, so not support to import.
        unsupported_columns = ['link', 'formula', 'link-formula']

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
        dst_columns = []

        for col in src_columns:
            col_key = col.get('key')
            col_type = col.get('type')
            col_name = col.get('name')
            col_data = col.get('data')
            if col_type in unsupported_columns:
                if col_key == '0000':
                    column_dict = {
                        'column_key': '0000',
                        'column_name': col_name,
                        'column_type': 'text',
                        'column_data': None
                    }
                else:
                    continue
            else:
                column_dict = generate_column(col)
                if not column_dict:
                    continue
            dst_columns.append(column_dict)

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
                raise Exception(error_msg)
        except Exception as e:
            error_msg = 'create dst table error: %s' % e
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
        try:
            error_info = json.loads(error_msg[error_msg.find('resp text: ')+11:])
        except:
            dtable_io_logger.exception(error_msg, exc_info=e)
        else:
            if error_info.get('error_type') == 'table_exist':
                dtable_io_logger.warning('table: %s exists in dtable: %s', dst_table_name, dst_dtable_uuid)
            else:
                dtable_io_logger.exception(error_msg)
        raise Exception(error_msg)
