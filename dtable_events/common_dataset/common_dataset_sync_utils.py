# -*- coding: utf-8 -*-
import logging
import os
import re
import sys
from copy import deepcopy

import requests
from dateutil import parser

from dtable_events.utils.constants import ColumnTypes

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
    import seahub.settings as seahub_settings
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
    SESSION_COOKIE_NAME = getattr(seahub_settings, 'SESSION_COOKIE_NAME', 'sessionid')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

dtable_server_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL


SRC_ROWS_LIMIT = 50000


DATA_NEED_KEY_VALUES = {
    ColumnTypes.DATE: [{
        'name': 'format',
        'optional_params': ['YYYY-MM-DD', 'M/D/YYYY', 'DD/MM/YYYY', 'YYYY-MM-DD HH:mm', 'DD.MM.YYYY', 'DD.MM.YYYY HH:mm', 'M/D/YYYY HH:mm'],
        'default': 'YYYY-MM-DD'
    }],
    ColumnTypes.DURATION: [{
        'name': 'duration_format',
        'optional_params': ['h:mm', 'h:mm:ss'],
        'default': 'h:mm'
    }, {
        'name': 'format',
        'optional_params': ['duration'],
        'default': 'duration'
    }],
    ColumnTypes.NUMBER: [{
        'name': 'format',
        'optional_params': ['number', 'percent', 'yuan', 'dollar', 'euro', 'custom_currency'],
        'default': 'number'
    }, {
        'name': 'decimal',
        'optional_params': ['comma', 'dot'],
        'default': 'dot'
    }, {
        'name': 'thousands',
        'optional_params': ['no', 'comma', 'dot', 'space'],
        'default': 'no'
    }],
    ColumnTypes.GEOLOCATION: [{
        'name': 'geo_format',
        'optional_params': ['geolocation', 'lng_lat', 'country_region', 'province_city_district', 'province', 'province_city'],
        'default': 'lng_lat'
    }]
}


def fix_column_data(column):
    data_need_key_values = DATA_NEED_KEY_VALUES.get(column['type'])
    if not data_need_key_values:
        return column
    for need_key_value in data_need_key_values:
        if need_key_value['name'] not in column['data']:
            column['data'][need_key_value['name']] = need_key_value['default']
        else:
            if column['data'][need_key_value['name']] not in need_key_value['optional_params']:
                column['data'][need_key_value['name']] = need_key_value['default']
    return column


def transfer_link_formula_array_column(column, array_type, array_data):
    if not array_type:
        column['type'] = ColumnTypes.TEXT
        column['data'] = None
    elif array_type in [
        ColumnTypes.NUMBER,
        ColumnTypes.DATE,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.DURATION,
        ColumnTypes.GEOLOCATION,
        ColumnTypes.RATE,
    ]:
        column['type'] = array_type
        column['data'] = array_data
        if column['type'] not in [ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT] and array_data is not None:
            column = fix_column_data(column)
    elif array_type in [
        ColumnTypes.TEXT,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.IMAGE,
        ColumnTypes.FILE,
        ColumnTypes.EMAIL,
        ColumnTypes.URL,
        ColumnTypes.CHECKBOX,
        ColumnTypes.CREATOR,
        ColumnTypes.CTIME,
        ColumnTypes.LAST_MODIFIER,
        ColumnTypes.MTIME,
    ]:
        column['type'] = array_type
        column['data'] = None
    else:
        column['type'] = ColumnTypes.TEXT
        column['data'] = None
    return column


def transfer_column(src_column):
    """
    transfer origin column to new target column
    """
    if src_column.get('type') == ColumnTypes.BUTTON:
        return None
    column = deepcopy(src_column)
    if column.get('type') in [
        ColumnTypes.DATE,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.GEOLOCATION
    ]:
        """
        Because these column types need specific keys and values in column['data'],
        need to fix column data result of dtable version iteration
        """
        if column.get('data'):
            column = fix_column_data(column)
    if src_column.get('type') == ColumnTypes.AUTO_NUMBER:
        column['type'] = ColumnTypes.TEXT
        column['data'] = None
    elif src_column.get('type') == ColumnTypes.FORMULA:
        data = src_column.get('data', {})
        result_type = data.get('result_type', 'string')
        if result_type == 'date':
            column['type'] = ColumnTypes.DATE
            column['data'] = {
                'format': data.get('format', 'YYYY-MM-DD')
            }
        elif result_type == 'number':
            column['type'] = ColumnTypes.NUMBER
            column['data'] = {
                'format': data.get('format', 'number'),
                'precision': data.get('precision', 2),
                'enable_precision': data.get('enable_precision', False),
                'enable_fill_default_value': data.get('enable_fill_default_value', False),
                'decimal': data.get('decimal', 'dot'),
                'thousands': data.get('thousands', 'no'),
                'currency_symbol': data.get('currency_symbol')
            }
            column = fix_column_data(column)
        elif result_type == 'bool':
            column['type'] = ColumnTypes.CHECKBOX
            column['data'] = None
        else:
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
    elif src_column.get('type') == ColumnTypes.LINK:
        data = src_column.get('data') or {}
        array_type = data.get('array_type')
        array_data = data.get('array_data')
        column = transfer_link_formula_array_column(column, array_type, array_data)
    elif src_column.get('type') == ColumnTypes.LINK_FORMULA:
        data = src_column.get('data') or {}
        result_type = data.get('result_type', 'string')
        if result_type == 'number':
            column['type'] = ColumnTypes.NUMBER
            column['data'] = {
                'format': data.get('format', 'number'),
                'precision': data.get('precision', 2),
                'enable_precision': data.get('enable_precision', False),
                'enable_fill_default_value': data.get('enable_fill_default_value', False),
                'decimal': data.get('decimal', 'dot'),
                'thousands': data.get('thousands', 'no'),
                'currency_symbol': data.get('currency_symbol')
            }
        elif result_type == 'string':
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
        elif result_type == 'date':
            column['type'] = ColumnTypes.DATE
            column['data'] = {
                'format': data.get('format', 'YYYY-MM-DD')
            }
        elif result_type == 'bool':
            column['type'] = ColumnTypes.CHECKBOX,
            column['data'] = None
        elif result_type == 'array':
            array_type = data.get('array_type')
            array_data = data.get('array_data')
            column = transfer_link_formula_array_column(column, array_type, array_data)
        else:
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
    return column


def generate_synced_columns(src_columns, dst_columns=None):
    """
    generate synced columns
    return: to_be_updated_columns -> list or None, to_be_appended_columns -> list or None, error_msg -> str or None
    """
    transfered_columns = []
    for col in src_columns:
        new_col = transfer_column(col)
        if new_col:
            transfered_columns.append(new_col)
    if not dst_columns:
        return None, transfered_columns, None
    to_be_updated_columns, to_be_appended_columns = [], []
    dst_column_name_dict = {col.get('name'): True for col in dst_columns}
    dst_column_key_dict = {col.get('key'): col for col in dst_columns}

    for col in transfered_columns:
        dst_col = dst_column_key_dict.get(col.get('key'))
        if dst_col:
            dst_col['type'] = col.get('type')
            dst_col['data'] = col.get('data')
            to_be_updated_columns.append(dst_col)
        else:
            if dst_column_name_dict.get(col.get('name')):
                return None, None, 'Column %s exists' % (col.get('name'),)
            to_be_appended_columns.append(col)
    return to_be_updated_columns, to_be_appended_columns, None


def generate_synced_rows(converted_rows, src_rows, src_columns, synced_columns, dst_rows=None):
    """
    generate synced rows divided into `rows to be updated`, `rows to be appended` and `rows to be deleted`
    return: to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids
    """

    converted_rows_dict = {row.get('_id'): row for row in converted_rows}
    src_rows_dict = {row.get('_id'): row for row in src_rows}
    synced_columns_dict = {col.get('key'): col for col in synced_columns}

    to_be_updated_rows, to_be_appended_rows, transfered_row_ids = [], [], {}
    if not dst_rows:
        dst_rows = []
    to_be_deleted_row_ids = []
    for row in dst_rows:
        row_id = row.get('_id')
        src_row = src_rows_dict.get(row_id)
        converted_row = converted_rows_dict.get(row_id)
        if not converted_row or not src_row:
            to_be_deleted_row_ids.append(row_id)
            continue

        update_row = generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=row)
        if update_row:
            update_row['_id'] = row_id
            to_be_updated_rows.append(update_row)
        transfered_row_ids[row_id] = True

    for converted_row in converted_rows:
        row_id = converted_row.get('_id')
        src_row = src_rows_dict.get(row_id)
        if not src_row or transfered_row_ids.get(row_id):
            continue
        append_row = generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=None)
        if append_row:
            append_row['_id'] = row_id
            to_be_appended_rows.append(append_row)
        transfered_row_ids[row_id] = True

    return to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids


def get_link_formula_converted_cell_value(transfered_column, converted_cell_value, src_col_type):
    transfered_type = transfered_column.get('type')
    if not isinstance(converted_cell_value, list):
        return
    if src_col_type == ColumnTypes.LINK:
        converted_cell_value = [v['display_value'] for v in converted_cell_value]
    if transfered_type in [
        ColumnTypes.TEXT,
        ColumnTypes.RATE,
        ColumnTypes.NUMBER,
        ColumnTypes.DURATION,
        ColumnTypes.EMAIL,
        ColumnTypes.CHECKBOX,
        ColumnTypes.AUTO_NUMBER,
        ColumnTypes.CREATOR,
        ColumnTypes.CTIME,
        ColumnTypes.LAST_MODIFIER,
        ColumnTypes.MTIME,
        ColumnTypes.URL,
        ColumnTypes.GEOLOCATION,
        ColumnTypes.SINGLE_SELECT
    ]:
        if converted_cell_value:
            return converted_cell_value[0]
    elif transfered_type == ColumnTypes.COLLABORATOR:
        if converted_cell_value:
            if isinstance(converted_cell_value[0], list):
                return list(set(converted_cell_value[0]))
            else:
                return list(set(converted_cell_value))
    elif transfered_type in [
        ColumnTypes.IMAGE,
        ColumnTypes.FILE
    ]:
        if converted_cell_value:
            if isinstance(converted_cell_value[0], list):
                return converted_cell_value[0]
            else:
                return converted_cell_value
    elif transfered_type == ColumnTypes.LONG_TEXT:
        if converted_cell_value:
            return converted_cell_value[0]
    elif transfered_type == ColumnTypes.MULTIPLE_SELECT:
        if converted_cell_value:
            if isinstance(converted_cell_value[0], list):
                return sorted(list(set(converted_cell_value[0])))
            else:
                return sorted(list(set(converted_cell_value)))
    elif transfered_type == ColumnTypes.DATE:
        if converted_cell_value:
            try:
                value = parser.isoparse(converted_cell_value[0])
            except:
                pass
            else:
                data_format = transfered_column.get('data', {}).get('format')
                if data_format == 'YYYY-MM-DD':
                    return value.strftime('%Y-%m-%d')
                elif data_format == 'YYYY-MM-DD HH:mm':
                    return value.strftime('%Y-%m-%d %H:%M')
                else:
                    return value.strftime('%Y-%m-%d')


def get_converted_cell_value(converted_cell_value, src_row, transfered_column, col):
    col_key = col.get('key')
    col_type = col.get('type')
    if col_type in [
        ColumnTypes.TEXT,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.IMAGE,
        ColumnTypes.FILE,
        ColumnTypes.RATE,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.DURATION,
        ColumnTypes.EMAIL,
        ColumnTypes.DATE,
        ColumnTypes.CHECKBOX,
        ColumnTypes.AUTO_NUMBER,
        ColumnTypes.CREATOR,
        ColumnTypes.CTIME,
        ColumnTypes.LAST_MODIFIER,
        ColumnTypes.MTIME,
        ColumnTypes.URL,
        ColumnTypes.GEOLOCATION
    ]:
        return deepcopy(src_row.get(col_key))

    elif col_type == ColumnTypes.SINGLE_SELECT:
        if not isinstance(converted_cell_value, str):
            return
        return converted_cell_value

    elif col_type == ColumnTypes.MULTIPLE_SELECT:
        if not isinstance(converted_cell_value, list):
            return
        return converted_cell_value

    elif col_type == ColumnTypes.LINK:
        return get_link_formula_converted_cell_value(transfered_column, converted_cell_value, col_type)
    elif col_type == ColumnTypes.FORMULA:
        result_type = col.get('data', {}).get('result_type')
        if result_type == 'number':
            re_number = r'(\-|\+)?\d+(\.\d+)?'
            try:
                match_obj = re.search(re_number, str(converted_cell_value))
                if not match_obj:
                    return
                start, end = match_obj.span()
                return float(str(converted_cell_value)[start: end])
            except Exception as e:
                logger.error('re search: %s in: %s error: %s', re_number, converted_cell_value, e)
                return
        elif result_type == 'date':
            return converted_cell_value
        elif result_type == 'bool':
            if isinstance(converted_cell_value, bool):
                return converted_cell_value
            return str(converted_cell_value).upper() == 'TRUE'
        elif result_type == 'string':
            col_data = col.get('data', {})
            options = col_data.get('options') if col_data else None
            if options and isinstance(options, list):
                options_dict = {option.get('id'): option.get('name', '') for option in options}
                if isinstance(converted_cell_value, list):
                    values = [options_dict.get(item, item) for item in converted_cell_value]
                    return ', '.join(values)
                else:
                    return options_dict.get(converted_cell_value, converted_cell_value)
            else:
                if isinstance(converted_cell_value, list):
                    return ', '.join(str(v) for v in converted_cell_value)
                elif isinstance(converted_cell_value, dict):
                    return ', '.join(str(converted_cell_value.get(v)) for v in converted_cell_value)
                else:
                    return converted_cell_value
        else:
            if isinstance(converted_cell_value, list):
                return ', '.join(str(v) for v in converted_cell_value)
            else:
                return converted_cell_value

    elif col_type == ColumnTypes.LINK_FORMULA:
        result_type = col.get('data', {}).get('result_type')
        if result_type == 'number':
            re_number = r'(\-|\+)?\d+(\.\d+)?'
            try:
                match_obj = re.search(re_number, str(converted_cell_value))
                if not match_obj:
                    return
                start, end = match_obj.span()
                if '.' not in str(converted_cell_value)[start: end]:
                    return int(str(converted_cell_value)[start: end])
                else:
                    return float(str(converted_cell_value)[start: end])
            except Exception as e:
                logger.error('re search: %s in: %s error: %s', re_number, converted_cell_value, e)
                return
        elif result_type == 'date':
            return converted_cell_value
        elif result_type == 'bool':
            if isinstance(converted_cell_value, bool):
                return converted_cell_value
            return str(converted_cell_value).upper() == 'TRUE'
        elif result_type == 'array':
            return get_link_formula_converted_cell_value(transfered_column, converted_cell_value, col_type)
        elif result_type == 'string':
            if converted_cell_value:
                return str(converted_cell_value)
    return src_row.get(col_key)


def is_equal(v1, v2, column_type):
    """
    judge two values equal or not
    different column types -- different judge method
    """
    try:
        if column_type in [
            ColumnTypes.TEXT,
            ColumnTypes.DATE,
            ColumnTypes.SINGLE_SELECT,
            ColumnTypes.URL,
            ColumnTypes.CREATOR,
            ColumnTypes.LAST_MODIFIER,
            ColumnTypes.CTIME,
            ColumnTypes.MTIME,
            ColumnTypes.EMAIL
        ]:
            v1 = v1 if v1 else ''
            v2 = v2 if v2 else ''
            return v1 == v2
        elif column_type == ColumnTypes.CHECKBOX:
            v1 = True if v1 else False
            v2 = True if v2 else False
            return v1 == v2
        elif column_type == ColumnTypes.DURATION:
            return v1 == v2
        elif column_type == ColumnTypes.NUMBER:
            return v1 == v2
        elif column_type == ColumnTypes.RATE:
            return v1 == v2
        elif column_type == ColumnTypes.COLLABORATOR:
            return v1 == v2
        elif column_type == ColumnTypes.IMAGE:
            return v1 == v2
        elif column_type == ColumnTypes.FILE:
            files1 = [file['url'] for file in v1] if v1 else []
            files2 = [file['url'] for file in v2] if v2 else []
            return files1 == files2
        elif column_type == ColumnTypes.LONG_TEXT:
            if v1 is not None:
                if isinstance(v1, dict):
                    v1 = v1['text']
                else:
                    v1 = str(v1)
            if v2 is not None:
                if isinstance(v2, dict):
                    v2 = v2['text']
                else:
                    v2 = str(v2)
            return v1 == v2
        elif column_type == ColumnTypes.MULTIPLE_SELECT:
            if v1 is not None and isinstance(v1, list):
                v1 = sorted(v1)
            if v2 is not None and isinstance(v2, list):
                v2 = sorted(v2)
            return v1 == v2
        else:
            return v1 == v2
    except Exception as e:
        logger.exception(e)
        logger.error('sync common dataset value v1: %s, v2: %s type: %s error: %s', v1, v2, column_type, e)
        return False


def generate_single_row(converted_row, src_row, src_columns, transfered_columns_dict, dst_row=None):
    """
    generate new single row according to src column type
    :param converted_row: {'_id': '', 'column_name_1': '', 'col_name_2'; ''} from dtable-db
    :param src_columns: [{'key': 'column_key_1', 'name': 'column_name_1'}]
    :param transfered_columns_dict: {'col_key_1': {'key': 'column_key_1', 'name': 'column_name_1'}}
    :param dst_row: {'_id': '', 'column_key_1': '', 'col_key_2': ''}
    """
    dataset_row = {}
    op_type = 'update'
    if not dst_row:
        op_type = 'append'
    dst_row = deepcopy(dst_row) if dst_row else {'_id': src_row.get('_id')}
    for col in src_columns:
        col_key = col.get('key')

        converted_cell_value = converted_row.get(col_key)
        transfered_column = transfered_columns_dict.get(col_key)
        if not transfered_column:
            continue

        if op_type == 'update':
            converted_cell_value = get_converted_cell_value(converted_cell_value, src_row, transfered_column, col)
            if not is_equal(dst_row.get(col_key), converted_cell_value, transfered_column['type']):
                dataset_row[col_key] = converted_cell_value
        else:
            dataset_row[col_key] = get_converted_cell_value(converted_cell_value, src_row, transfered_column, col)

    return dataset_row


def import_or_sync(import_sync_context):
    """
    import or sync common dataset
    return: {
        dst_table_id: destination table id,
        error_msg: error msg,
        task_status_code: return frontend status code, 40x 50x...
    }
    """
    # extract necessary assets
    dst_dtable_uuid = import_sync_context.get('dst_dtable_uuid')
    src_dtable_uuid = import_sync_context.get('src_dtable_uuid')

    src_rows = import_sync_context.get('src_rows')
    src_columns = import_sync_context.get('src_columns')
    src_column_keys_set = {col['key'] for col in src_columns}
    src_table_name = import_sync_context.get('src_table_name')
    src_view_name = import_sync_context.get('src_view_name')
    src_headers = import_sync_context.get('src_headers')

    dst_table_id = import_sync_context.get('dst_table_id')
    dst_table_name = import_sync_context.get('dst_table_name')
    dst_headers = import_sync_context.get('dst_headers')
    dst_columns = import_sync_context.get('dst_columns')
    dst_rows = import_sync_context.get('dst_rows')

    lang = import_sync_context.get('lang', 'en')

    # generate cols and rows
    ## Old generate-cols is from src_columns from dtable json, but some (link-)formula columns have wrong array_type
    ## For example, a LOOKUP(GEOLOCATION) link-formula column, whose array_type in dtable json is `string`, but should being `GEOLOCATION`
    ## So, generate columns from the columns(archive columns) returned by SQL query instead of from the columns in dtable json, and remove old code
    ## New code and more details is in the following code

    ## generate rows
    ### get src view-rows
    result_rows = []
    start, limit = 0, 10000
    to_be_updated_columns, to_be_appended_columns = [], []

    while True:
        url = dtable_server_url.rstrip('/') + '/api/v1/internal/dtables/' + str(src_dtable_uuid) + '/view-rows/?from=dtable_events'
        if (start + limit) > SRC_ROWS_LIMIT:
            limit = SRC_ROWS_LIMIT - start
        query_params = {
            'table_name': src_table_name,
            'view_name': src_view_name,
            'use_dtable_db': True,
            'start': start,
            'limit': limit
        }
        try:
            resp = requests.get(url, headers=src_headers, params=query_params)
            if resp.status_code == 400:
                try:
                    res_json = resp.json()
                except:
                    return {
                        'dst_table_id': None,
                        'error_msg': 'fetch src view rows error',
                        'task_status_code': 500
                    }
                else:
                    return {
                        'dst_table_id': None,
                        'error_msg': 'fetch src view rows error',
                        'error_type': res_json.get('error_type'),
                        'task_status_code': 400
                    }
            res_json = resp.json()
            archive_rows = res_json.get('rows', [])
            archive_metadata = res_json.get('metadata')
        except Exception as e:
            logger.error('request src_dtable: %s params: %s view-rows error: %s', src_dtable_uuid, query_params, e)
            return {
                'dst_table_id': None,
                'error_msg': 'fetch view rows error',
                'task_status_code': 500
            }
        if start == 0:
            ## generate columns from the columns(archive_metadata) returned from SQL query
            sync_columns = [col for col in archive_metadata if col['key'] in src_column_keys_set]
            to_be_updated_columns, to_be_appended_columns, error = generate_synced_columns(sync_columns, dst_columns=dst_columns)
            if error:
                return {
                    'dst_table_id': None,
                    'error_msg': str(error),  # generally, this error is caused by client
                    'task_status_code': 400
                }
        result_rows.extend(archive_rows)
        if not archive_rows or len(archive_rows) < limit or (start + limit) >= SRC_ROWS_LIMIT:
            break
        start += limit

    final_columns = (to_be_updated_columns or []) + (to_be_appended_columns or [])

    to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids = generate_synced_rows(result_rows, src_rows, src_columns, final_columns, dst_rows=dst_rows)

    # sync table
    ## maybe create table
    if not dst_table_id:
        url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/tables/' % (str(dst_dtable_uuid),)
        data = {
            'table_name': dst_table_name,
            'lang': lang,
            'columns': [{
                'column_key': col.get('key'),
                'column_name': col.get('name'),
                'column_type': col.get('type'),
                'column_data': col.get('data')
            } for col in to_be_appended_columns] if to_be_appended_columns else []
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('create new table error status code: %s, resp text: %s', resp.status_code, resp.text)
                error_msg = 'create table error'
                status_code = 500
                try:
                    resp_json = resp.json()
                    if resp_json.get('error_message'):
                        error_msg = resp_json['error_message']
                    status_code = resp.status_code
                except:
                    pass
                return {
                    'dst_table_id': None,
                    'error_msg': error_msg,
                    'task_status_code': status_code
                }
            dst_table_id = resp.json().get('_id')
        except Exception as e:
            logger.error(e)
            return {
                'dst_table_id': None,
                'error_msg': 'create table error',
                'task_status_code': 500
            }
    ## or maybe append/update columns
    else:
        ### batch append columns
        if to_be_appended_columns:
            url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-append-columns/?from=dtable_events'
            data = {
                'table_id': dst_table_id,
                'columns': [{
                    'column_key': col.get('key'),
                    'column_name': col.get('name'),
                    'column_type': col.get('type'),
                    'column_data': col.get('data')
                } for col in to_be_appended_columns]
            }
            try:
                resp = requests.post(url, headers=dst_headers, json=data)
                if resp.status_code != 200:
                    logger.error('batch append columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                    return {
                        'dst_table_id': None,
                        'error_msg': 'append columns error',
                        'task_status_code': 500
                    }
            except Exception as e:
                logger.error('batch append columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'append columns error',
                    'task_status_code': 500
                }
        ### batch update columns
        if to_be_updated_columns:
            url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-update-columns/?from=dtable_events'
            data = {
                'table_id': dst_table_id,
                'columns': [{
                    'key': col.get('key'),
                    'type': col.get('type'),
                    'data': col.get('data')
                } for col in to_be_updated_columns]
            }
            try:
                resp = requests.put(url, headers=dst_headers, json=data)
                if resp.status_code != 200:
                    logger.error('batch update columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                    return {
                        'dst_table_id': None,
                        'error_msg': 'update columns error',
                        'task_status_code': 500
                    }
            except Exception as e:
                logger.error('batch update columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'update columns error',
                    'task_status_code': 500
                }

    ## update delete append rows step by step
    step = 1000
    ### update rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-update-rows/?from=dtable_events' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_updated_rows), step):
        updates = []
        for row in to_be_updated_rows[i: i+step]:
            updates.append({
                'row_id': row['_id'],
                'row': row
            })
        data = {
            'table_name': dst_table_name,
            'updates': updates,
            'need_convert_back': False
        }
        try:
            resp = requests.put(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('sync dataset update rows dst dtable: %s dst table: %s error status code: %s content: %s', dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
                return {
                    'dst_table_id': None,
                    'error_msg': 'update rows error',
                    'task_status_code': 500
                }
        except Exception as e:
            logger.error('sync dataset update rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return {
                'dst_table_id': None,
                'error_msg': 'update rows error',
                'task_status_code': 500
            }

    ### delete rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-delete-rows/?from=dtable_events' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_deleted_row_ids), step):
        data = {
            'table_name': dst_table_name,
            'row_ids': to_be_deleted_row_ids[i: i+step]
        }
        try:
            resp = requests.delete(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('sync dataset delete rows dst dtable: %s dst table: %s error status code: %s, content: %s', dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
                return {
                    'dst_table_id': None,
                    'error_msg': 'delete rows error',
                    'task_status_code': 500
                }
        except Exception as e:
            logger.error('sync dataset delete rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return {
                'dst_table_id': None,
                'error_msg': 'delete rows error',
                'task_status_code': 500
            }

    ### append rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-append-rows/' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_appended_rows), step):
        data = {
            'table_name': dst_table_name,
            'rows': to_be_appended_rows[i: i+step],
            'need_convert_back': False
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('sync dataset append rows dst dtable: %s dst table: %s error status code: %s', dst_dtable_uuid, dst_table_name, resp.status_code)
                return {
                    'dst_table_id': None,
                    'error_msg': 'append rows error',
                    'task_status_code': 500
                }
        except Exception as e:
            logger.error('sync dataset append rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return {
                'dst_table_id': None,
                'error_msg': 'append rows error',
                'task_status_code': 500
            }

    return {
        'dst_table_id': dst_table_id,
        'error_msg': None,
        'task_status_code': 200
    }


def set_common_dataset_invalid(dataset_id, db_session):
    sql = "UPDATE dtable_common_dataset SET is_valid=0 WHERE id=:dataset_id"
    try:
        db_session.execute(sql, {'dataset_id': dataset_id})
        db_session.commit()
    except Exception as e:
        logger.error('set state of common dataset: %s error: %s', dataset_id, e)


def set_common_dataset_sync_invalid(dataset_sync_id, db_session):
    sql = "UPDATE dtable_common_dataset_sync SET is_valid=0 WHERE id=:dataset_sync_id"
    try:
        db_session.execute(sql, {'dataset_sync_id': dataset_sync_id})
        db_session.commit()
    except Exception as e:
        logger.error('set state of common dataset sync: %s error: %s', dataset_sync_id, e)

