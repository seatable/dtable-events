# -*- coding: utf-8 -*-
import logging
import os
import re
import sys
from copy import deepcopy

import requests
from dateutil import parser

from dtable_events.utils.sql_generator import BaseSQLGenerator
from dtable_events.app.config import INNER_DTABLE_DB_URL
from dtable_events.common_dataset.dtable_db_cell_validators import validate_table_db_cell_value
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)

dtable_server_url = get_inner_dtable_server_url()


SRC_ROWS_LIMIT = 50000
INSERT_UPDATE_ROWS_LIMIT = 1000
DELETE_ROWS_LIMIT = 10000


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
    }],
    ColumnTypes.SINGLE_SELECT: [{
        'name': 'options',
        'default': []
    }],
    ColumnTypes.MULTIPLE_SELECT: [{
        'name': 'options',
        'default': []
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
            if need_key_value.get('optional_params') and column['data'][need_key_value['name']] not in need_key_value['optional_params']:
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
        if array_data is not None:
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
        ColumnTypes.GEOLOCATION,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT
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


def generate_synced_rows(converted_rows, src_columns, synced_columns, dst_rows=None, to_archive=False):
    """
    generate synced rows divided into `rows to be updated`, `rows to be appended` and `rows to be deleted`
    return: to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids
    """

    converted_rows_dict = {row.get('_id'): row for row in converted_rows}
    synced_columns_dict = {col.get('key'): col for col in synced_columns}

    to_be_updated_rows, to_be_appended_rows, transfered_row_ids = [], [], {}
    if not dst_rows:
        dst_rows = []
    to_be_deleted_row_ids = []
    for row in dst_rows:
        row_id = row.get('_id')
        converted_row = converted_rows_dict.get(row_id)
        if not converted_row:
            to_be_deleted_row_ids.append(row_id)
            continue

        update_row = generate_single_row(converted_row, src_columns, synced_columns_dict, dst_row=row, to_archive=to_archive)
        if update_row:
            update_row['_id'] = row_id
            to_be_updated_rows.append(update_row)
        transfered_row_ids[row_id] = True

    for converted_row in converted_rows:
        row_id = converted_row.get('_id')
        if transfered_row_ids.get(row_id):
            continue
        append_row = generate_single_row(converted_row, src_columns, synced_columns_dict, dst_row=None, to_archive=to_archive)
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


def get_converted_cell_value(converted_cell_value, transfered_column, col):
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
        return deepcopy(converted_cell_value)

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
    return deepcopy(converted_cell_value)


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
                    v1 = v1.get('text', '')
                else:
                    v1 = str(v1)
            if v2 is not None:
                if isinstance(v2, dict):
                    v2 = v2.get('text', '')
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


def generate_single_row(converted_row, src_columns, transfered_columns_dict, dst_row=None, to_archive=False):
    """
    generate new single row according to src column type
    :param converted_row: {'_id': '', 'column_key_1': '', 'col_key_2'; ''} from dtable-db
    :param src_columns: [{'key': 'column_key_1', 'name': 'column_name_1'}]
    :param transfered_columns_dict: {'col_key_1': {'key': 'column_key_1', 'name': 'column_name_1'}}
    :param dst_row: {'_id': '', 'column_key_1': '', 'col_key_2': ''}
    :param to_archive: is row for dtable-db

    :return: dataset_row => if to_archive is True {col_name1: value1,...} else {col_key1: value1,...}
    """
    dataset_row = {}
    op_type = 'update'
    if not dst_row:
        op_type = 'append'
    dst_row = deepcopy(dst_row) if dst_row else {'_id': converted_row.get('_id')}
    for col in src_columns:
        col_key = col.get('key')

        converted_cell_value = converted_row.get(col_key)
        transfered_column = transfered_columns_dict.get(col_key)
        if not transfered_column:
            continue

        if to_archive and col['key'] in ['_creator', '_ctime', '_last_modifier', '_mtime']:
            continue

        if op_type == 'update':
            converted_cell_value = get_converted_cell_value(converted_cell_value, transfered_column, col)
            if not is_equal(dst_row.get(col_key), converted_cell_value, transfered_column['type']):
                if not to_archive:
                    dataset_row[col_key] = converted_cell_value
                else:
                    dataset_row[col['name']] = validate_table_db_cell_value(transfered_column, converted_cell_value)
        else:
            converted_cell_value = get_converted_cell_value(converted_cell_value, transfered_column, col)
            if not to_archive:
                dataset_row[col_key] = converted_cell_value
            else:
                dataset_row[col['name']] = validate_table_db_cell_value(transfered_column, converted_cell_value)

    return dataset_row


def create_dst_table_or_update_columns(dst_dtable_uuid, dst_table_id, dst_table_name, to_be_appended_columns, to_be_updated_columns, dst_dtable_server_api, lang):
    if not dst_table_id:  ## create table
        columns = [{
            'column_key': col.get('key'),
            'column_name': col.get('name'),
            'column_type': col.get('type'),
            'column_data': col.get('data')
        } for col in to_be_appended_columns] if to_be_appended_columns else []
        try:
            resp_json = dst_dtable_server_api.add_table(dst_table_name, lang, columns=columns)
            dst_table_id = resp_json.get('_id')
        except Exception as e:
            logger.error(e)  # TODO: table exists shoud return 400
            return None, {
                'dst_table_id': None,
                'error_msg': 'create table error',
                'task_status_code': 500
            }
    else:  ## append/update columns
        ### batch append columns
        if to_be_appended_columns:
            columns = [{
                'column_key': col.get('key'),
                'column_name': col.get('name'),
                'column_type': col.get('type'),
                'column_data': col.get('data')
            } for col in to_be_appended_columns]
            try:
                dst_dtable_server_api.batch_append_columns_by_table_id(dst_table_id, columns)
            except Exception as e:
                logger.error('batch append columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
                return None, {
                    'dst_table_id': None,
                    'error_msg': 'append columns error',
                    'task_status_code': 500
                }
        ### batch update columns
        if to_be_updated_columns:
            columns = [{
                'key': col.get('key'),
                'type': col.get('type'),
                'data': col.get('data')
            } for col in to_be_updated_columns]
            try:
                dst_dtable_server_api.batch_update_columns_by_table_id(dst_table_id, columns)
            except Exception as e:
                logger.error('batch update columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
                return None, {
                    'dst_table_id': None,
                    'error_msg': 'update columns error',
                    'task_status_code': 500
                }
    return dst_table_id, None


def append_dst_rows(dst_dtable_uuid, dst_table_name, to_be_appended_rows, dst_dtable_db_api, dst_dtable_server_api, to_archive):
    if to_archive:
        step = 10000
        for i in range(0, len(to_be_appended_rows), step):
            try:
                dst_dtable_db_api.insert_rows(dst_table_name, to_be_appended_rows[i: i+step])
            except Exception as e:
                logger.error('sync dataset append rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'append rows error',
                    'task_status_code': 500
                }
    else:
        step = INSERT_UPDATE_ROWS_LIMIT
        for i in range(0, len(to_be_appended_rows), step):
            try:
                dst_dtable_server_api.batch_append_rows(dst_table_name, to_be_appended_rows[i: i+step], need_convert_back=False)
            except Exception as e:
                logger.error('sync dataset append rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'append rows error',
                    'task_status_code': 500
                }


def update_dst_rows(dst_dtable_uuid, dst_table_name, to_be_updated_rows, dst_dtable_db_api, dst_dtable_server_api, to_archive):
    if to_archive:
        step = 10000
        for i in range(0, len(to_be_updated_rows), step):
            updates = []
            for row in to_be_updated_rows[i: i+step]:
                row_id = row.pop('_id', None)
                updates.append({
                    'row_id': row_id,
                    'row': row
                })
            try:
                dst_dtable_db_api.batch_update_rows(dst_table_name, updates)
            except Exception as e:
                logger.error('sync dataset update rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'update rows error',
                    'task_status_code': 500
                }
    else:
        step = INSERT_UPDATE_ROWS_LIMIT
        for i in range(0, len(to_be_updated_rows), step):
            updates = [{
                'row_id': row['_id'],
                'row': row
            } for row in to_be_updated_rows[i: i+step]]
            try:
                dst_dtable_server_api.batch_update_rows(dst_table_name, updates, need_convert_back=False)
            except Exception as e:
                logger.error('sync dataset update rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'update rows error',
                    'task_status_code': 500
                }


def delete_dst_rows(dst_dtable_uuid, dst_table_name, to_be_deleted_row_ids, dst_dtable_db_api, dst_dtable_server_api, to_archive):
    if to_archive:
        step = 10000
        for i in range(0, len(to_be_deleted_row_ids), step):
            try:
                dst_dtable_db_api.batch_delete_rows(dst_table_name, to_be_deleted_row_ids[i: i+step])
            except Exception as e:
                logger.error('sync dataset delete rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
    else:
        step = DELETE_ROWS_LIMIT
        for i in range(0, len(to_be_deleted_row_ids), step):
            try:
                dst_dtable_server_api.batch_delete_rows(dst_table_name, to_be_deleted_row_ids[i: i+step])
            except Exception as e:
                logger.error('sync dataset delete rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)


def import_sync_CDS(context):
    """
    import or sync common dataset
    please check all resources in context before call this function

    Steps:
        1. create or update dst columns
        2. fetch src rows, (find rows to be updated and rows to be appended, update and append them), step by step
        3. fetch dst rows, (find rows to be deleted, delete them), step by step
    """
    src_dtable_uuid = context.get('src_dtable_uuid')
    dst_dtable_uuid = context.get('dst_dtable_uuid')

    src_table_name = context.get('src_table_name')
    src_view_name = context.get('src_view_name')
    src_view_type = context.get('src_view_type', 'table')
    src_columns = context.get('src_columns')
    src_enable_archive = context.get('src_enable_archive', False)

    dst_table_id = context.get('dst_table_id')
    dst_table_name = context.get('dst_table_name')
    dst_columns = context.get('dst_columns')

    operator = context.get('operator')
    lang = context.get('lang', 'en')

    to_archive = context.get('to_archive', False)

    src_dtable_server_api = DTableServerAPI(operator, src_dtable_uuid, dtable_server_url)
    src_dtable_db_api = DTableDBAPI(operator, src_dtable_uuid, INNER_DTABLE_DB_URL)
    dst_dtable_server_api = DTableServerAPI(operator, dst_dtable_uuid, dtable_server_url)
    dst_dtable_db_api = DTableDBAPI(operator, dst_dtable_uuid, INNER_DTABLE_DB_URL)

    server_only = not (to_archive and src_enable_archive and src_view_type == 'archive')
    is_sync = bool(dst_table_id)
    logger.debug('to_archive: %s src_enable_archive: %s src_view_type: %s', to_archive, src_enable_archive, src_view_type)

    src_column_keys_set = {col['key'] for col in src_columns}

    # fetch src rows, find existed rows, not existed rows, update/append rows, step by step
    start, step = 0, 10000
    src_row_ids_set = set()
    to_be_updated_columns, to_be_appended_columns = [], []
    final_columns = []
    while True:
        logger.debug('update/append start: %s step: %s', start, step)
        if server_only and (start + step) > SRC_ROWS_LIMIT:
            step = SRC_ROWS_LIMIT - start
        try:
            res_json = src_dtable_server_api.internal_view_rows(src_table_name, src_view_name, use_dtable_db=True, server_only=server_only, start=start, limit=step)
            step_src_rows = res_json.get('rows', [])
            src_view_metadata = res_json.get('metadata')
        except Exception as e:
            logger.error('request src_dtable: %s view-rows error: %s', src_dtable_uuid, e)
            return {
                'dst_table_id': None,
                'error_msg': 'fetch view rows error',
                'task_status_code': 500
            }
        if start == 0: ## generate columns from the columns(archive_metadata) returned from SQL query in internal_view_rows
            sync_columns = [col for col in src_view_metadata if col['key'] in src_column_keys_set]
            to_be_updated_columns, to_be_appended_columns, error = generate_synced_columns(sync_columns, dst_columns=dst_columns)
            if error:
                return {
                    'dst_table_id': None,
                    'error_type': 'generate_synced_columns_error',
                    'error_msg': str(error),  # generally, this error is caused by client
                    'task_status_code': 400
                }
            final_columns = (to_be_updated_columns or []) + (to_be_appended_columns or [])
            ### create or update dst columns
            dst_table_id, error_resp = create_dst_table_or_update_columns(dst_dtable_uuid, dst_table_id, dst_table_name, to_be_appended_columns, to_be_updated_columns, dst_dtable_server_api, lang)
            if error_resp:
                return error_resp
        row_ids = []
        step_rows_dict = {}
        for row in step_src_rows:
            if row['_id'] in src_row_ids_set:
                continue
            row_ids.append(row['_id'])
            src_row_ids_set.add(row['_id'])
            step_rows_dict[row['_id']] = row
        if not row_ids:
            if not step_src_rows or len(step_src_rows) < step or (server_only and (start + step) >= SRC_ROWS_LIMIT):
                break
            start += step
            continue
        ## find to-be-appended-rows to-be-updated-rows
        step_dst_rows = None
        if dst_table_id:
            sql = "SELECT _id, %(dst_columns)s FROM `%(dst_table)s` WHERE _id IN %(row_ids)s LIMIT %(rows_count)s" % {
                'dst_table': dst_table_name,
                'dst_columns': ', '.join(["`%s`" % col['name'] for col in final_columns]),
                'row_ids': '(%s)' % ', '.join(["'%s'" % row_id for row_id in row_ids]),
                'rows_count': len(row_ids)
            }
            try:
                step_dst_rows = dst_dtable_db_api.query(sql, convert=False, server_only=(not to_archive))
            except Exception as e:
                logger.error('find to-be-updated-rows error: %s', e)
                return {
                    'dst_table_id': None,
                    'error_msg': 'find to-be-updated-rows error',
                    'task_status_code': 500
                }
        logger.debug('step_dst_rows: %s', len(step_dst_rows))
        filtered_step_src_rows = [step_rows_dict[row_id] for row_id in row_ids]
        to_be_updated_rows, to_be_appended_rows, _ = generate_synced_rows(filtered_step_src_rows, src_columns, final_columns, dst_rows=step_dst_rows, to_archive=to_archive)
        logger.debug('to_be_updated_rows: %s to_be_appended_rows: %s', len(to_be_updated_rows), len(to_be_appended_rows))
        ## append
        if to_be_appended_rows:
            error_resp = append_dst_rows(dst_dtable_uuid, dst_table_name, to_be_appended_rows, dst_dtable_db_api, dst_dtable_server_api, to_archive)
            if error_resp:
                return error_resp
        ## update
        if to_be_updated_rows:
            error_resp = update_dst_rows(dst_dtable_uuid, dst_table_name, to_be_updated_rows, dst_dtable_db_api, dst_dtable_server_api, to_archive)
            if error_resp:
                return error_resp
        ## judge whether break
        if not step_src_rows or len(step_src_rows) < step or (server_only and (start + step) >= SRC_ROWS_LIMIT):
            break
        start += step

    # fetch dst rows, find useless rows, delete rows, step by step
    dst_row_ids_set = set()
    start, step = 0, 10000
    ## generate src view WHERE clause
    try:
        src_metadata = src_dtable_server_api.get_metadata()
    except Exception as e:
        logger.error('request src dtable: %s table: %s metadata error: %s', src_dtable_uuid, src_table_name, e)
        return {
            'dst_table_id': None,
            'error_msg': 'request src table metadata error',
            'task_status_code': 500
        }
    src_table = [table for table in src_metadata['tables'] if table['name'] == src_table_name][0]
    src_view = [view for view in src_table['views'] if view['name'] == src_view_name][0]
    filters = src_view.get('filters', [])
    filter_conjunction = src_view.get('filter_conjunction', 'And')
    filter_conditions = {
        'filters': filters,
        'filter_conjunction': filter_conjunction
    }
    sql_generator = BaseSQLGenerator(src_table_name, src_table['columns'], filter_conditions=filter_conditions)
    filter_clause = sql_generator._filter2sql()
    ## delete useless rows step by step
    while is_sync and True:
        logger.debug('delete start: %s step: %s', start, step)
        sql = "SELECT _id FROM `%(dst_table_name)s` LIMIT %(start)s, %(limit)s" % {
            'dst_table_name': dst_table_name,
            'start': start,
            'limit': step
        }
        rows = dst_dtable_db_api.query(sql, convert=False, server_only=(not to_archive))
        query_row_ids_set = set()
        for row in rows:
            if row['_id'] in dst_row_ids_set:
                continue
            if row['_id'] in src_row_ids_set:
                continue
            query_row_ids_set.add(row['_id'])
            dst_row_ids_set.add(row['_id'])
        if not query_row_ids_set:
            if len(rows) < step:
                break
            start += step
            continue
        sql = "SELECT _id FROM `%(src_table_name)s` WHERE _id IN %(row_ids)s %(view_filter_clause)s LIMIT %(rows_count)s" % {
            'src_table_name': src_table_name,
            'row_ids': '(%s)' % ', '.join(["'%s'" % row_id for row_id in query_row_ids_set]),
            'rows_count': len(query_row_ids_set),
            'view_filter_clause': 'AND (%s)' % filter_clause[filter_clause.find('WHERE') + len('WHERE'):] if filter_clause else ''
        }
        existed_rows = src_dtable_db_api.query(sql, convert=False, server_only=server_only)
        to_be_deleted_row_ids_set = query_row_ids_set - {row['_id'] for row in existed_rows}
        if to_be_deleted_row_ids_set:
            delete_dst_rows(dst_dtable_uuid, dst_table_name, list(to_be_deleted_row_ids_set), dst_dtable_db_api, dst_dtable_server_api, to_archive)
        if len(rows) < step:
                break
        start += step

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
