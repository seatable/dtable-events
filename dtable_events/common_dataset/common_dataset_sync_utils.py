# -*- coding: utf-8 -*-
import logging
import re
import traceback
from copy import deepcopy
from datetime import datetime

from sqlalchemy import text
from dateutil import parser

from dtable_events.app.config import INNER_DTABLE_DB_URL
from dtable_events.app.log import setup_logger
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_36_chars, uuid_str_to_32_chars
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import BaseExceedsException, DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.sql_generator import BaseSQLGenerator, SQLGeneratorOptionInvalidError, ColumnFilterInvalidError

cds_logger = setup_logger('dtable_events_cds', propagate=False)

dtable_server_url = get_inner_dtable_server_url()


SRC_ROWS_LIMIT = 50000
INSERT_UPDATE_ROWS_LIMIT = 1000
DELETE_ROWS_LIMIT = 10000
INVALID_WARNING_ROWS = 10


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
    elif array_type in [
        ColumnTypes.DEPARTMENT_SINGLE_SELECT
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


def is_column_need_update(col, dst_col):
    if dst_col.get('type') != col.get('type'):
        return True

    col_data = col.get('data') or {}
    dst_col_data = dst_col.get('data') or {}

    if col['type'] in [
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
        ColumnTypes.DIGITAL_SIGN,
        ColumnTypes.DEPARTMENT_SINGLE_SELECT
    ]:
        return False

    elif col['type'] == ColumnTypes.RATE:
        keys = ['rate_max_number', 'rate_style_color', 'rate_style_type']
        return not all([col_data.get(key) == dst_col_data.get(key) for key in keys])

    elif col['type'] == ColumnTypes.DATE:
        return col_data.get('format') != dst_col_data.get('format')

    elif col['type'] == ColumnTypes.NUMBER:
        keys = ['format', 'decimal', 'thousands']
        return not all([col_data.get(key) == dst_col_data.get(key) for key in keys])

    elif col['type'] == ColumnTypes.GEOLOCATION:
        keys = ['geo_format']
        return not all([col_data.get(key) == dst_col_data.get(key) for key in keys])

    elif col['type'] == ColumnTypes.DURATION:
        keys = ['duration_format', 'format']
        return not all([col_data.get(key) == dst_col_data.get(key) for key in keys])

    elif col['type'] in [
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT
    ]:
        col_data_options = col_data.get('options') or []
        dst_col_data_options = dst_col_data.get('options') or []
        if len(col_data_options) != len(dst_col_data_options):
            return True
        col_data_options = sorted(col_data_options, key=lambda option: option['id'])
        dst_col_data_options = sorted(dst_col_data_options, key=lambda option: option['id'])
        option_keys = ['id', 'name', 'color']
        for col_option, dst_col_option in zip(col_data_options, dst_col_data_options):
            if all([col_option.get(key) == dst_col_option.get(key) for key in option_keys]):
                continue
            return True
        return False

    return True


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
        return None, transfered_columns, None, None
    to_be_updated_columns, to_be_appended_columns, to_be_kept_columns = [], [], []
    dst_column_name_dict = {col.get('name'): True for col in dst_columns}
    dst_column_key_dict = {col.get('key'): col for col in dst_columns}

    for col in transfered_columns:
        dst_col = dst_column_key_dict.get(col.get('key'))
        if dst_col:
            if is_column_need_update(col, dst_col):
                dst_col['type'] = col.get('type')
                dst_col['data'] = col.get('data')
                to_be_updated_columns.append(dst_col)
            else:
                to_be_kept_columns.append(dst_col)
        else:
            if dst_column_name_dict.get(col.get('name')):
                column_name = col.get('name')
                err_msg = f"column {column_name} exists"
                return None, None, None, err_msg
            to_be_appended_columns.append(col)
    return to_be_updated_columns, to_be_appended_columns, to_be_kept_columns, None


def append_rows_invalid_infos(rows_invalid_infos: list, row_invalid_info):
    if len(rows_invalid_infos) >= INVALID_WARNING_ROWS:
        return
    rows_invalid_infos.append(row_invalid_info)


def generate_synced_rows(dtable_db_rows, src_columns, synced_columns, rows_invalid_infos, dst_rows=None):
    """
    generate synced rows divided into `rows to be updated`, `rows to be appended` and `rows to be deleted`
    return: to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids
    """

    dtable_db_rows_dict = {row.get('_id'): row for row in dtable_db_rows}
    synced_columns_dict = {col.get('key'): col for col in synced_columns}
    to_be_updated_rows, to_be_appended_rows, transfered_row_ids = [], [], {}
    if not dst_rows:
        dst_rows = []
    for row in dst_rows:
        row_id = row.get('_id')
        dtable_db_row = dtable_db_rows_dict.get(row_id)
        if not dtable_db_row:
            continue

        update_row, row_invalid_info = generate_single_row(dtable_db_row, src_columns, synced_columns_dict, dst_row=row)
        if update_row:
            update_row['_id'] = row_id
            to_be_updated_rows.append(update_row)
        if row_invalid_info.get('invalid_cell_infos'):
            append_rows_invalid_infos(rows_invalid_infos, row_invalid_info)
        transfered_row_ids[row_id] = True

    for dtable_db_row in dtable_db_rows:
        row_id = dtable_db_row.get('_id')
        if transfered_row_ids.get(row_id):
            continue
        append_row, row_invalid_info = generate_single_row(dtable_db_row, src_columns, synced_columns_dict, dst_row=None)
        if append_row:
            append_row['_id'] = row_id
            to_be_appended_rows.append(append_row)
        if row_invalid_info.get('invalid_cell_infos'):
            append_rows_invalid_infos(rows_invalid_infos, row_invalid_info)
        transfered_row_ids[row_id] = True

    return to_be_updated_rows, to_be_appended_rows


def get_link_formula_converted_cell_value(transfered_column, dtable_db_cell_value, src_col_type):
    transfered_type = transfered_column.get('type')
    if not isinstance(dtable_db_cell_value, list):
        return
    if src_col_type == ColumnTypes.LINK:
        dtable_db_cell_value = [v['display_value'] for v in dtable_db_cell_value]
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
        if dtable_db_cell_value:
            return dtable_db_cell_value[0]
    elif transfered_type == ColumnTypes.COLLABORATOR:
        if dtable_db_cell_value:
            if isinstance(dtable_db_cell_value[0], list):
                return list(set(dtable_db_cell_value[0]))
            else:
                return list(set(dtable_db_cell_value))
    elif transfered_type in [
        ColumnTypes.IMAGE,
        ColumnTypes.FILE
    ]:
        if dtable_db_cell_value:
            if isinstance(dtable_db_cell_value[0], list):
                return dtable_db_cell_value[0]
            else:
                return dtable_db_cell_value
    elif transfered_type == ColumnTypes.LONG_TEXT:
        if dtable_db_cell_value:
            return dtable_db_cell_value[0]
    elif transfered_type == ColumnTypes.MULTIPLE_SELECT:
        if dtable_db_cell_value:
            if isinstance(dtable_db_cell_value[0], list):
                return sorted(list(set(dtable_db_cell_value[0])))
            else:
                return sorted(list(set(dtable_db_cell_value)))
    elif transfered_type == ColumnTypes.DATE:
        if dtable_db_cell_value:
            try:
                value = parser.isoparse(dtable_db_cell_value[0])
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
    elif transfered_type == ColumnTypes.DEPARTMENT_SINGLE_SELECT:
        if dtable_db_cell_value:
            try:
                return int(dtable_db_cell_value[0])
            except:
                pass


def get_converted_cell_value(dtable_db_cell_value, transfered_column, col):
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
        return deepcopy(dtable_db_cell_value)

    elif col_type == ColumnTypes.DEPARTMENT_SINGLE_SELECT:
        try:
            dtable_db_cell_value = int(dtable_db_cell_value)  # department-single-select need a int
        except:
            return
        return dtable_db_cell_value

    elif col_type == ColumnTypes.SINGLE_SELECT:
        if not isinstance(dtable_db_cell_value, str):
            return
        return dtable_db_cell_value

    elif col_type == ColumnTypes.MULTIPLE_SELECT:
        if not isinstance(dtable_db_cell_value, list):
            return
        return dtable_db_cell_value

    elif col_type == ColumnTypes.LINK:
        return get_link_formula_converted_cell_value(transfered_column, dtable_db_cell_value, col_type)
    elif col_type == ColumnTypes.FORMULA:
        result_type = col.get('data', {}).get('result_type')
        if result_type == 'number':
            if type(dtable_db_cell_value) in (float, int):
                return dtable_db_cell_value
            re_number = r'(\-|\+)?\d+(\.\d+)?(e(\-|\+)?\d+)?'
            try:
                match_obj = re.search(re_number, str(dtable_db_cell_value))
                if not match_obj:
                    return
                start, end = match_obj.span()
                return float(str(dtable_db_cell_value)[start: end])
            except Exception as e:
                cds_logger.error('re search: %s in: %s error: %s', re_number, dtable_db_cell_value, e)
                return
        elif result_type == 'date':
            return dtable_db_cell_value
        elif result_type == 'bool':
            if isinstance(dtable_db_cell_value, bool):
                return dtable_db_cell_value
            return str(dtable_db_cell_value).upper() == 'TRUE'
        elif result_type == 'string':
            col_data = col.get('data', {})
            options = col_data.get('options') if col_data else None
            if options and isinstance(options, list):
                options_dict = {option.get('id'): option.get('name', '') for option in options}
                if isinstance(dtable_db_cell_value, list):
                    values = [options_dict.get(item, item) for item in dtable_db_cell_value]
                    return ', '.join(values)
                else:
                    return options_dict.get(dtable_db_cell_value, dtable_db_cell_value)
            else:
                if isinstance(dtable_db_cell_value, list):
                    return ', '.join(str(v) for v in dtable_db_cell_value)
                elif isinstance(dtable_db_cell_value, dict):
                    return ', '.join(str(dtable_db_cell_value.get(v)) for v in dtable_db_cell_value)
                else:
                    return dtable_db_cell_value
        else:
            if isinstance(dtable_db_cell_value, list):
                return ', '.join(str(v) for v in dtable_db_cell_value)
            else:
                return dtable_db_cell_value

    elif col_type == ColumnTypes.LINK_FORMULA:
        result_type = col.get('data', {}).get('result_type')
        if result_type == 'number':
            if type(dtable_db_cell_value) in (float, int):
                return dtable_db_cell_value
            re_number = r'(\-|\+)?\d+(\.\d+)?(e(\-|\+)?\d+)?'
            try:
                match_obj = re.search(re_number, str(dtable_db_cell_value))
                if not match_obj:
                    return
                start, end = match_obj.span()
                if '.' not in str(dtable_db_cell_value)[start: end]:
                    return int(str(dtable_db_cell_value)[start: end])
                else:
                    return float(str(dtable_db_cell_value)[start: end])
            except Exception as e:
                cds_logger.error('re search: %s in: %s error: %s', re_number, dtable_db_cell_value, e)
                return
        elif result_type == 'date':
            return dtable_db_cell_value
        elif result_type == 'bool':
            if isinstance(dtable_db_cell_value, bool):
                return dtable_db_cell_value
            return str(dtable_db_cell_value).upper() == 'TRUE'
        elif result_type == 'array':
            return get_link_formula_converted_cell_value(transfered_column, dtable_db_cell_value, col_type)
        elif result_type == 'string':
            if dtable_db_cell_value:
                return str(dtable_db_cell_value)
    return deepcopy(dtable_db_cell_value)


EXPECT_VALUE_TYPES = {
    ColumnTypes.TEXT: [str],
    ColumnTypes.IMAGE: [list],
    ColumnTypes.DATE: [str],
    ColumnTypes.LONG_TEXT: [str],
    ColumnTypes.CHECKBOX: [bool],
    ColumnTypes.SINGLE_SELECT: [str],
    ColumnTypes.MULTIPLE_SELECT: [list],
    ColumnTypes.URL: [str],
    ColumnTypes.DURATION: [int, float],
    ColumnTypes.NUMBER: [int, float],
    ColumnTypes.FILE: [list],
    ColumnTypes.COLLABORATOR: [list],
    ColumnTypes.EMAIL: [str],
    ColumnTypes.CREATOR: [str],
    ColumnTypes.LAST_MODIFIER: [str],
    ColumnTypes.CTIME: [str],
    ColumnTypes.MTIME: [str],
    ColumnTypes.RATE: [int],
    ColumnTypes.GEOLOCATION: [dict],
    ColumnTypes.DEPARTMENT_SINGLE_SELECT: [int]
}


def get_converted_cell_value_with_check(dtable_db_cell_value, transfered_column, col):
    value = get_converted_cell_value(dtable_db_cell_value, transfered_column, col)
    column_type = transfered_column.get('type', ColumnTypes.TEXT)
    invalid_cell_info = None
    if column_type not in EXPECT_VALUE_TYPES:
        invalid_cell_info = {
            'column_key': col['key'],
            'src_column_type': col['type'],
            'dst_column_type': transfered_column['type'],
            'src_column_name': col['name'],
            'dst_column_name': col['name'],
            'invalid_type': 'column_type_invalid'
        }
        value = None
    else:
        expect_types = EXPECT_VALUE_TYPES.get(column_type)
        if value is not None:
            if not isinstance(value, tuple(expect_types)):
                invalid_cell_info = {
                    'column_key': col['key'],
                    'src_column_type': col['type'],
                    'dst_column_type': transfered_column['type'],
                    'src_column_name': col['name'],
                    'dst_column_name': col['name'],
                    'invalid_type': 'cell_type_invalid',
                    'value': value
                }
                value = None
    return value, invalid_cell_info


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
                v1 = sorted([item for item in v1 if item])
            if v2 is not None and isinstance(v2, list):
                v2 = sorted([item for item in v2 if item])
            return v1 == v2
        else:
            return v1 == v2
    except Exception as e:
        cds_logger.exception(e)
        cds_logger.error('sync common dataset value v1: %s, v2: %s type: %s error: %s', v1, v2, column_type, e)
        return False


def generate_single_row(dtable_db_row, src_columns, transfered_columns_dict, dst_row=None):
    """
    generate new single row according to src column type
    :param dtable_db_row: {'_id': '', 'col_key_1': '', 'col_key_2'; ''} from dtable-db
    :param src_columns: [{'key': 'column_key_1', 'name': 'column_name_1'}]
    :param transfered_columns_dict: {'col_key_1': {'key': 'column_key_1', 'name': 'column_name_1'}}
    :param dst_row: {'_id': '', 'col_key_1': '', 'col_key_2': ''}

    :return: dataset_row => {col_key1: value1,...}
    """
    dataset_row = {}
    op_type = 'update'
    if not dst_row:
        op_type = 'append'
    dst_row = deepcopy(dst_row) if dst_row else {'_id': dtable_db_row.get('_id')}
    invalid_cell_infos = []
    for col in src_columns:
        col_key = col.get('key')

        dtable_db_cell_value = dtable_db_row.get(col_key)
        transfered_column = transfered_columns_dict.get(col_key)
        if not transfered_column:
            continue

        converted_cell_value, invalid_cell_info = get_converted_cell_value_with_check(dtable_db_cell_value, transfered_column, col)
        if invalid_cell_info:
            invalid_cell_infos.append(invalid_cell_info)
        if op_type == 'update':
            if not is_equal(dst_row.get(col_key), converted_cell_value, transfered_column['type']):
                dataset_row[col_key] = converted_cell_value
        else:
            dataset_row[col_key] = converted_cell_value


    return dataset_row, {'invalid_cell_infos': invalid_cell_infos, 'row_id': dst_row['_id']}


def create_dst_table_or_update_columns(dst_dtable_uuid, dst_table_id, dst_table_name, to_be_appended_columns, to_be_updated_columns, dst_dtable_server_api, lang):
    if not dst_table_id:  ## create table
        columns = [{
            'column_key': col.get('key'),
            'column_name': col.get('name'),
            'column_type': col.get('type'),
            'column_data': col.get('data') or None
        } for col in to_be_appended_columns] if to_be_appended_columns else []
        try:
            resp_json = dst_dtable_server_api.add_table(dst_table_name, lang, columns=columns)
            dst_table_id = resp_json.get('_id')
        except BaseExceedsException as e:
            return None, {
                'dst_table_id': None,
                'error_msg': e.error_msg,
                'error_type': e.error_type,
                'task_status_code': 400
            }
        except Exception as e:
            cds_logger.error(e)  # TODO: table exists shoud return 400
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
            except BaseExceedsException as e:
                return None, {
                    'dst_table_id': None,
                    'error_msg': e.error_msg,
                    'error_type': e.error_type,
                    'task_status_code': 400
                }
            except Exception as e:
                cds_logger.error('batch append columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
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
            except BaseExceedsException as e:
                return None, {
                    'dst_table_id': None,
                    'error_msg': e.error_msg,
                    'error_type': e.error_type,
                    'task_status_code': 400
                }
            except Exception as e:
                cds_logger.error('batch update columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
                return None, {
                    'dst_table_id': None,
                    'error_msg': 'update columns error',
                    'task_status_code': 500
                }
    return dst_table_id, None


def append_dst_rows(dst_dtable_uuid, dst_table_name, to_be_appended_rows, dst_dtable_server_api, stats_info: dict):
    step = INSERT_UPDATE_ROWS_LIMIT
    for i in range(0, len(to_be_appended_rows), step):
        try:
            dst_dtable_server_api.batch_append_rows(dst_table_name, to_be_appended_rows[i: i+step], need_convert_back=False)
            stats_info['appended_rows_count'] += len(to_be_appended_rows[i: i+step])
        except BaseExceedsException as e:
            return {
                'dst_table_id': None,
                'error_msg': e.error_msg,
                'error_type': e.error_type,
                'task_status_code': 400
            }
        except Exception as e:
            cds_logger.error('sync dataset append rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return {
                'dst_table_id': None,
                'error_msg': 'append rows error',
                'task_status_code': 500
            }


def update_dst_rows(dst_dtable_uuid, dst_table_name, to_be_updated_rows, dst_dtable_server_api, stats_info: dict):
    step = INSERT_UPDATE_ROWS_LIMIT
    for i in range(0, len(to_be_updated_rows), step):
        updates = []
        for row in to_be_updated_rows[i: i+step]:
            row_id = row.pop('_id', None)
            updates.append({
                'row_id': row_id,
                'row': row
            })
        try:
            dst_dtable_server_api.batch_update_rows(dst_table_name, updates, need_convert_back=False)
            stats_info['updated_rows_count'] += len(to_be_updated_rows[i: i+step])
        except BaseExceedsException as e:
            return {
                'dst_table_id': None,
                'error_msg': e.error_msg,
                'error_type': e.error_type,
                'task_status_code': 400
            }
        except Exception as e:
            cds_logger.error('sync dataset update rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return {
                'dst_table_id': None,
                'error_msg': 'update rows error',
                'task_status_code': 500
            }


def delete_dst_rows(dst_dtable_uuid, dst_table_name, to_be_deleted_row_ids, dst_dtable_server_api, stats_info: dict):
    step = DELETE_ROWS_LIMIT
    for i in range(0, len(to_be_deleted_row_ids), step):
        try:
            dst_dtable_server_api.batch_delete_rows(dst_table_name, to_be_deleted_row_ids[i: i+step])
            stats_info['deleted_rows_count'] += len(to_be_deleted_row_ids[i: i+step])
        except Exception as e:
            cds_logger.error('sync dataset delete rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)


def get_dataset_data(dataset_id, src_dtable_uuid, src_table, src_view_id, server_only=True):
    """
    :return: dataset_data -> dict or None, error_body -> dict or None
    """
    src_dtable_db_api = DTableDBAPI('dtable-events', src_dtable_uuid, INNER_DTABLE_DB_URL)
    # fetch all src view rows id
    try:
        src_view = [view for view in src_table['views'] if view['_id'] == src_view_id][0]
    except IndexError:
        cds_logger.warning("src view %s not found" % src_view_id)
        return None, {
            'dst_table_id': None,
            'error_msg': 'view %s not found' % src_view_id,
            'task_status_code': 404
        }
    hidden_column_keys = src_view.get('hidden_columns') or []
    src_columns = [col for col in src_table['columns'] if col['key'] not in hidden_column_keys]
    filter_conditions = {
        'filters': src_view.get('filters', []),
        'filter_conjunction': src_view.get('filter_conjunction', 'And'),
        'sorts': src_view.get('sorts', [])
    }
    cds_logger.debug('filter_conditions: %s', filter_conditions)
    try:
        sql_generator = BaseSQLGenerator(src_table['name'], src_table['columns'], filter_conditions=filter_conditions)
        filter_clause = sql_generator._filter2sql()
        sort_clause = sql_generator._sort2sql()
    except ColumnFilterInvalidError as e:
        cds_logger.warning('src dtable: %s src table: %s src view: %s filter_conditions: %s to sql ColumnFilterInvalidError: %s', src_dtable_uuid, src_table['name'], src_view['_id'], filter_conditions, e)
        return None, {
            'dst_table_id': None,
            'error_msg': 'generate src view sql error: %s' % e,
            'error_type': 'wrong_filter_in_filters',
            'task_status_code': 400
        }
    except SQLGeneratorOptionInvalidError as e:
        cds_logger.warning('src dtable: %s src table: %s src view: %s filter_conditions: %s to sql option invalid error: %s', src_dtable_uuid, src_table['name'], src_view['_id'], filter_conditions, e)
        return None, {
            'dst_table_id': None,
            'error_msg': 'generate src view sql error: %s' % e,
            'error_type': 'wrong_filter_in_filters',
            'task_status_code': 400
        }
    except Exception as e:
        cds_logger.exception('src dtable: %s src table: %s src view: %s filter_conditions: %s to sql error: %s', src_dtable_uuid, src_table['name'], src_view['_id'], filter_conditions, e)
        return None, {
            'dst_table_id': None,
            'error_msg': 'generate src view sql error: %s' % e,
            'task_status_code': 500
        }
    rows_id_list, rows_dict = list(), dict()
    src_columns_str = ', '.join(map(lambda col: f"`{col['name']}`", src_columns))
    sql_template = f"SELECT `_id`, {src_columns_str} FROM `{src_table['name']}` {filter_clause or ''} {sort_clause or ''}"
    start, step = 0, 10000
    while True:
        sql = f"{sql_template} LIMIT {start}, {step}"
        cds_logger.debug('fetch src dtable: %s table: %s view: %s sql: %s', src_dtable_uuid, src_table['name'], src_view['_id'], sql[:200])
        try:
            rows, _ = src_dtable_db_api.query(sql, convert=False, server_only=server_only)
        except Exception as e:
            cds_logger.error('fetch src dtable: %s table: %s view: %s sql: %s error: %s', src_dtable_uuid, src_table['name'], src_view['_id'], sql[:200], e)
            return None, {
                'dst_table_id': None,
                'error_msg': 'fetch src rows id error: %s' % e,
                'task_status_code': 500
            }
        for row in rows:
            if row['_id'] in rows_dict:
                continue
            rows_dict[row['_id']] = row
            rows_id_list.append(row['_id'])
            if len(rows_id_list) >= SRC_ROWS_LIMIT:
                break
        cds_logger.info('dataset_id: %s src_dtable_uuid: %s src_table_id: %s src_view_id: %s fetched rows: %s', dataset_id, src_dtable_uuid, src_table['_id'], src_view_id, len(rows_id_list))
        if len(rows) < step or len(rows_id_list) >= SRC_ROWS_LIMIT:
            break
        start += step
    cds_logger.info('dataset_id: %s src_dtable_uuid: %s src_table_id: %s src_view_id: %s fetched rows: %s totally', dataset_id, src_dtable_uuid, src_table['_id'], src_view_id, len(rows_id_list))
    dataset_data = {'rows_id_list': rows_id_list, 'rows_dict': rows_dict}
    return dataset_data, None


def _import_sync_CDS(context):
    """
    fetch src/dst rows id, find need append/update/delete rows
    """

    dst_dtable_uuid = context.get('dst_dtable_uuid')

    src_table = context.get('src_table')
    src_view_id = context.get('src_view_id')

    dst_table_id = context.get('dst_table_id')
    dst_table_name = context.get('dst_table_name')
    dst_columns = context.get('dst_columns')

    operator = context.get('operator')
    lang = context.get('lang', 'en')

    dataset_data = context.get('dataset_data')

    stats_info = context.get('stats_info') or {}

    dst_dtable_server_api = DTableServerAPI(operator, dst_dtable_uuid, dtable_server_url)
    dst_dtable_db_api = DTableDBAPI(operator, dst_dtable_uuid, INNER_DTABLE_DB_URL)

    is_sync = bool(dst_table_id)

    # create dst table or update dst table columns
    # fetch all src view rows id, S
    # fetch all dst table rows id, D
    # to-be-appended-rows-id = S - D
    # to-be-updated-rows-id = S & D
    # to-be-deleted-rows-id = D - S
    # delete dst to-be-deleted-rows, step by step
    # fetch src to-be-updated-rows and dst to-be-updated-rows, update to dst table, step by step
    # fetch src to-be-append-rows, append to dst table, step by step

    src_view = [view for view in src_table['views'] if view['_id'] == src_view_id][0]
    hidden_column_keys = src_view.get('hidden_columns', [])
    src_columns = [col for col in src_table['columns'] if col['key'] not in hidden_column_keys]

    to_be_updated_columns, to_be_appended_columns, to_be_kept_columns, error = generate_synced_columns(src_columns, dst_columns=dst_columns)
    if error:
        return {
            'dst_table_id': None,
            'error_type': 'generate_synced_columns_error',
            'error_msg': str(error),  # generally, this error is caused by client
            'task_status_code': 400
        }
    final_columns = (to_be_updated_columns or []) + (to_be_appended_columns or []) + (to_be_kept_columns or [])
    stats_info['columns_count'] = len(final_columns)
    stats_info['link_formula_columns_count'] = len([col for col in src_table['columns'] if col['key'] not in hidden_column_keys and col['type'] == ColumnTypes.LINK_FORMULA])
    ### create or update dst columns
    dst_table_id, error_resp = create_dst_table_or_update_columns(dst_dtable_uuid, dst_table_id, dst_table_name, to_be_appended_columns, to_be_updated_columns, dst_dtable_server_api, lang)
    if error_resp:
        return error_resp

    # fetch all dst table rows id
    dst_rows_id_set = set()
    start, step = 0, 10000
    while is_sync and True:
        sql = f"SELECT `_id` FROM `{dst_table_name}` LIMIT {start}, {step}"
        cds_logger.debug('fetch dst rows-id sql: %s', sql[:200])
        try:
            rows, _ = dst_dtable_db_api.query(sql, convert=False, server_only=True)
        except Exception as e:
            cds_logger.error('fetch dst rows id sql: %s error: %s', sql[:200], e)
            return {
                'dst_table_id': None,
                'error_msg': 'fetch dst rows id error: %s' % e,
                'task_status_code': 500
            }
        dst_rows_id_set |= {row['_id'] for row in rows}
        if len(rows) < step:
            break
        start += step

    # calc to-be-appended-rows-id, to-be-updated-rows-id, to-be-deleted-rows-id
    to_be_appended_rows_id_set = dataset_data['rows_dict'].keys() - dst_rows_id_set
    to_be_updated_rows_id_set = dataset_data['rows_dict'].keys() & dst_rows_id_set
    to_be_deleted_rows_id_set = dst_rows_id_set - dataset_data['rows_dict'].keys()
    cds_logger.debug('to_be_appended_rows_id_set: %s, to_be_updated_rows_id_set: %s, to_be_deleted_rows_id_set: %s', len(to_be_appended_rows_id_set), len(to_be_updated_rows_id_set), len(to_be_deleted_rows_id_set))
    stats_info['to_be_appended_rows_count'] = len(to_be_appended_rows_id_set)
    stats_info['to_be_updated_rows_count'] = len(to_be_updated_rows_id_set)
    stats_info['to_be_deleted_rows_count'] = len(to_be_deleted_rows_id_set)

    # delete dst to-be-deleted-rows
    cds_logger.debug('will delete %s rows', len(to_be_deleted_rows_id_set))
    delete_dst_rows(dst_dtable_uuid, dst_table_name, list(to_be_deleted_rows_id_set), dst_dtable_server_api, stats_info)

    dst_query_columns = ', '.join(['_id'] + ["`%s`" % col['name'] for col in final_columns])

    rows_invalid_infos = []

    # fetch src to-be-updated-rows and dst to-be-updated-rows, update to dst table, step by step
    to_be_updated_rows_id_list = list(to_be_updated_rows_id_set)
    step = 10000
    for i in range(0, len(to_be_updated_rows_id_list), step):
        cds_logger.debug('to_be_updated_rows_id_list i: %s step: %s', i, step)
        src_rows = [dataset_data['rows_dict'][row_id] for row_id in to_be_updated_rows_id_list[i: i+step]]

        ## fetch dst to-be-updated-rows
        rows_id_str = ', '.join(["'%s'" % row_id for row_id in to_be_updated_rows_id_list[i: i+step]])
        sql = f"SELECT {dst_query_columns} FROM `{dst_table_name}` WHERE _id IN ({rows_id_str}) LIMIT {step}"
        try:
            dst_rows, _ = dst_dtable_db_api.query(sql, convert=False, server_only=True)
        except Exception as e:
            cds_logger.error('fetch dst to-be-updated-rows sql: %s error: %s', sql[:200], e)
            return {
                'dst_table_id': None,
                'error_msg': 'fetch dst to-be-updated-rows error: %s' % e,
                'task_status_code': 500
            }

        ## update
        to_be_updated_rows, _ = generate_synced_rows(src_rows, src_columns, final_columns, rows_invalid_infos, dst_rows=dst_rows)
        cds_logger.debug('step src update-rows: %s to-be-updated-rows: %s', len(to_be_updated_rows_id_list[i: i+step]), len(to_be_updated_rows))
        error_resp = update_dst_rows(dst_dtable_uuid, dst_table_name, to_be_updated_rows, dst_dtable_server_api, stats_info)
        if error_resp:
            return error_resp

    # fetch src to-be-append-rows, append to dst table, step by step
    ## this list is to record the order of src rows
    to_be_appended_rows_id_list = [row_id for row_id in dataset_data['rows_id_list'] if row_id in to_be_appended_rows_id_set]

    step = 10000
    for i in range(0, len(to_be_appended_rows_id_list), step):
        cds_logger.debug('to_be_appended_rows_id_list i: %s, step: %s', i, step)
        step_to_be_appended_rows_id_list = []
        step_row_sort_dict = {}
        for j in range(step):
            if i + j >= len(to_be_appended_rows_id_list):
                break
            step_to_be_appended_rows_id_list.append(to_be_appended_rows_id_list[i+j])
            step_row_sort_dict[to_be_appended_rows_id_list[i+j]] = j
        src_rows = [dataset_data['rows_dict'][row_id] for row_id in step_to_be_appended_rows_id_list]
        src_rows = sorted(src_rows, key=lambda row: step_row_sort_dict[row['_id']])
        _, to_be_appended_rows = generate_synced_rows(src_rows, src_columns, final_columns, rows_invalid_infos)
        error_resp = append_dst_rows(dst_dtable_uuid, dst_table_name, to_be_appended_rows, dst_dtable_server_api, stats_info)
        if error_resp:
            return error_resp

    if rows_invalid_infos:
        logs = [f"dst_dtable_uuid: {dst_dtable_uuid} dst_table_id: {dst_table_id} src_view_id: {src_view_id} some invalid cells warnings"]
        for row_invalid_info in rows_invalid_infos:
            row_id = row_invalid_info['row_id']
            invalid_cell_infos = row_invalid_info['invalid_cell_infos']
            logs.append(f"\trow: {row_id}")
            for invalid_cell_info in invalid_cell_infos:
                logs.append(f"\t\t{invalid_cell_info}")
        cds_logger.warning('\n'.join(logs))

    return {
        'dst_table_id': dst_table_id,
        'error_msg': '',
        'task_status_code': 200
    }


def import_sync_CDS(context):
    stats_sender = context.get('app')._stats_sender
    stats_info = stats_sender.get_stats_cds_info_template()

    stats_info['org_id'] = context.get('org_id')
    stats_info['dataset_id'] = int(context.get('dataset_id'))
    stats_info['dataset_sync_id'] = context.get('dataset_sync_id')
    stats_info['src_dtable_uuid'] = uuid_str_to_32_chars(context.get('src_dtable_uuid'))
    stats_info['src_table_id'] = (context.get('src_table') or {}).get('_id')
    stats_info['src_view_id'] = context.get('src_view_id')
    stats_info['dst_dtable_uuid'] = uuid_str_to_32_chars(context.get('dst_dtable_uuid'))
    if context.get('dst_table_id'):
        stats_info['import_or_sync'] = 'sync'
        stats_info['dst_table_id'] = context.get('dst_table_id')
    else:
        stats_info['import_or_sync'] = 'import'
        stats_info['dst_table_id'] = None
    stats_info['operator'] = context.get('operator')
    started_at = datetime.now()
    stats_info['started_at'] = started_at.isoformat()

    context['stats_info'] = stats_info

    cds_logger.info('start to sync dataset_id: %s sync_id: %s src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s operator: %s', 
                stats_info['dataset_id'], 
                stats_info['dataset_sync_id'],
                stats_info['src_dtable_uuid'],
                stats_info['src_table_id'],
                stats_info['src_view_id'],
                stats_info['dst_dtable_uuid'],
                stats_info['dst_table_id'],
                stats_info['operator'])
    try:
        result = _import_sync_CDS(context)
    except Exception as e:
        stats_info['is_success'] = False
        stats_info['error'] = traceback.format_exc()
        raise e
    else:
        stats_info['dst_table_id'] = result.get('dst_table_id')
        stats_info['is_success'] = True
    finally:
        finished_at = datetime.now()
        stats_info['finished_at'] = finished_at.isoformat()
        cds_logger.info('finished sync dataset_id: %s sync_id: %s src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s operator: %s cost: %ss', 
                    stats_info['dataset_id'],
                    stats_info['dataset_sync_id'],
                    stats_info['src_dtable_uuid'],
                    stats_info['src_table_id'],
                    stats_info['src_view_id'],
                    stats_info['dst_dtable_uuid'],
                    stats_info['dst_table_id'],
                    stats_info['operator'],
                    (finished_at - started_at).seconds)
        cds_logger.info('finished sync dataset_id: %s sync_id: %s detail: %s',
                    stats_info['dataset_id'],
                    stats_info['dataset_sync_id'],
                    stats_info)
        stats_sender.send(stats_sender.CDS_CHANNEL, stats_info)
    return result


def set_common_dataset_invalid(dataset_id, db_session):
    sql = "UPDATE dtable_common_dataset SET is_valid=0 WHERE id=:dataset_id"
    try:
        db_session.execute(text(sql), {'dataset_id': dataset_id})
        db_session.commit()
    except Exception as e:
        cds_logger.error('set state of common dataset: %s error: %s', dataset_id, e)


def set_common_dataset_syncs_invalid(dataset_sync_ids, db_session):
    sql = "UPDATE dtable_common_dataset_sync SET is_valid=0 WHERE id IN :dataset_sync_ids"
    try:
        db_session.execute(text(sql), {'dataset_sync_ids': dataset_sync_ids})
        db_session.commit()
    except Exception as e:
        cds_logger.error('set state of common dataset sync: %s error: %s', dataset_sync_ids, e)


def gen_src_assets(src_dtable_uuid, src_table_id, src_view_id, dataset_sync_ids, db_session):
    """
    :return: assets -> dict
    """
    dtable_server_url = get_inner_dtable_server_url()
    src_dtable_server_api = DTableServerAPI('dtable-events', src_dtable_uuid, dtable_server_url)
    try:
        src_dtable_metadata = src_dtable_server_api.get_metadata()
    except Exception as e:
        cds_logger.error('request src dst dtable: %s metadata error: %s', src_dtable_uuid, e)
        return None
    src_table, src_view = None, None
    for table in src_dtable_metadata.get('tables', []):
        if table['_id'] == src_table_id:
            src_table = table
            break
    if not src_table:
        set_common_dataset_syncs_invalid(dataset_sync_ids, db_session)
        cds_logger.warning('src: %s, %s, %s Source table not found.', src_dtable_uuid, src_table_id, src_view_id)
        return None
    for view in src_table.get('views', []):
        if view['_id'] == src_view_id:
            src_view = view
            break
    if not src_view:
        set_common_dataset_syncs_invalid(dataset_sync_ids, db_session)
        cds_logger.warning('src: %s, %s, %s Source view not found.', src_dtable_uuid, src_table_id, src_view_id)
        return None

    src_version = src_dtable_metadata.get('version')

    # query org_id
    sql = "SELECT w.org_id FROM workspaces w JOIN dtables d ON w.id=d.workspace_id WHERE d.uuid=:dtable_uuid"
    results = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(src_dtable_uuid)})
    src_workspace = next(results, None)
    org_id = src_workspace.org_id

    return {
        'src_table': src_table,
        'src_version': src_version,
        'org_id': org_id
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
        cds_logger.error('request src dst dtable: %s metadata error: %s', dst_dtable_uuid, e)
        return None
    dst_table = None
    for table in dst_dtable_metadata.get('tables', []):
        if table['_id'] == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        set_common_dataset_syncs_invalid([dataset_sync_id], db_session)
        cds_logger.warning('sync: %s destination table not found.', dataset_sync_id)
        return None
    return {
        'dst_table_name': dst_table['name'],
        'dst_columns': dst_table['columns']
    }


def batch_sync_common_dataset(app, dataset_id, dataset_syncs, db_session, is_force_sync=False, operator='dtable-events'):
    """
    batch sync CDS content to all syncs

    :params dataset_syncs: a list of object with properties `sync_id`, `dst_dtable_uuid`, `dst_table_id`, `src_version`
    """
    # fetch src assets
    src_dtable_uuid = uuid_str_to_36_chars(dataset_syncs[0].src_dtable_uuid)
    src_table_id = dataset_syncs[0].src_table_id
    src_view_id = dataset_syncs[0].src_view_id
    sync_ids = [dataset_sync.sync_id for dataset_sync in dataset_syncs]
    src_assets = gen_src_assets(src_dtable_uuid, src_table_id, src_view_id, sync_ids, db_session)
    if not src_assets:
        cds_logger.info('sync dataset_id: %s break!', dataset_id)
        return
    src_table = src_assets.get('src_table')

    dataset_data = None
    for dataset_sync in dataset_syncs:
        dst_dtable_uuid = uuid_str_to_36_chars(dataset_sync.dst_dtable_uuid)
        dst_table_id = dataset_sync.dst_table_id
        dataset_sync_id = dataset_sync.sync_id
        last_src_version = dataset_sync.src_version

        dst_assets = gen_dst_assets(dst_dtable_uuid, dst_table_id, dataset_sync_id, db_session)

        if not dst_assets:
            cds_logger.info('sync dataset_id: %s sync_id: %s break!', dataset_id, dataset_sync_id)
            continue

        if not is_force_sync and src_assets.get('src_version') == last_src_version:
            cds_logger.info('sync dataset_id: %s sync_id: %s break, src last version not changed!', dataset_id, dataset_sync_id)
            continue

        src_table = src_assets.get('src_table')
        dst_table_name = dst_assets.get('dst_table_name')

        if not dataset_data:
            try:
                dataset_data, error = get_dataset_data(dataset_id, src_dtable_uuid, src_table, src_view_id)
            except Exception as e:
                cds_logger.exception('request dtable: %s table: %s view: %s data error: %s', src_dtable_uuid, src_table_id, src_view_id, e)
            if error:
                if error.get('task_status_code') == 400 and error.get('error_type') == 'wrong_filter_in_filters':
                    set_common_dataset_syncs_invalid(sync_ids, db_session)
                cds_logger.error('request dtable: %s table: %s view: %s data error: %s', src_dtable_uuid, src_table_id, src_view_id, error)
                cds_logger.info('sync dataset_id: %s break!', dataset_id)
                return

        try:
            result = import_sync_CDS({
                'dataset_id': dataset_id,
                'dataset_sync_id': dataset_sync_id,
                'src_dtable_uuid': src_dtable_uuid,
                'dst_dtable_uuid': dst_dtable_uuid,
                'src_table': src_table,
                'src_view_id': src_view_id,
                'dst_table_id': dst_table_id,
                'dst_table_name': dst_table_name,
                'dst_columns': dst_assets.get('dst_columns'),
                'operator': operator or 'dtable-events',
                'lang': 'en',  # TODO: lang
                'dataset_data': dataset_data,
                'org_id': src_assets.get('org_id'),
                'db_session': db_session,
                'app': app
            })
        except Exception as e:
            cds_logger.error('sync common dataset src-uuid: %s src-table: %s src-view: %s dst-uuid: %s dst-table: %s error: %s', 
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
                    cds_logger.warning('src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s client error: %s',
                                    src_dtable_uuid, src_table_id, src_view_id, dst_dtable_uuid, dst_table_id, result)
                    set_common_dataset_syncs_invalid([dataset_sync_id], db_session)
                else:
                    cds_logger.error('src_dtable_uuid: %s src_table_id: %s src_view_id: %s dst_dtable_uuid: %s dst_table_id: %s error: %s',
                                src_dtable_uuid, src_table_id, src_view_id, dst_dtable_uuid, dst_table_id, result)
                continue
        sql = '''
            UPDATE dtable_common_dataset_sync SET last_sync_time=:last_sync_time, src_version=:src_version
            WHERE id=:id
        '''

        db_session.execute(text(sql), {
            'last_sync_time': datetime.now(),
            'src_version': src_assets.get('src_version'),
            'id': dataset_sync_id
        })
        db_session.commit()
