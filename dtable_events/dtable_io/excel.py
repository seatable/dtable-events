import re
import json
from datetime import datetime, time

from openpyxl import load_workbook
import csv

import logging
import os
import sys
import openpyxl
from openpyxl.styles import PatternFill
from dtable_events.utils import utc_to_tz
from dtable_events.utils.constants import ColumnTypes

# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

sys.path.insert(0, dtable_web_dir)
try:
    from seahub.settings import EXPORT2EXCEL_DEFAULT_STRING
except ImportError as err:
    EXPORT2EXCEL_DEFAULT_STRING = 'illegal character in excel'
    logging.warning('Can not import seahub.settings: %s.' % err)

# CONF DIR
central_conf_dir, timezone = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', ''), 'UTC'
if central_conf_dir:
    sys.path.insert(0, central_conf_dir)
    try:
        import dtable_web_settings
        timezone = getattr(dtable_web_settings, 'TIME_ZONE', 'UTC')
    except Exception as e:
        logging.error('import dtable_web_settings error: %s', e)
    else:
        del dtable_web_settings
else:
    logging.error('no conf dir SEAFILE_CENTRAL_CONF_DIR find')

VIRTUAL_ID_EMAIL_DOMAIN = '@auth.local'

first_grouped_row_fill = PatternFill(fill_type='solid', fgColor='ffa18b')
second_grouped_row_fill = PatternFill(fill_type='solid', fgColor='ffff4d')
third_grouped_row_fill = PatternFill(fill_type='solid', fgColor='a5f89b')
grouped_row_fills = [first_grouped_row_fill, second_grouped_row_fill, third_grouped_row_fill]


CHECKBOX_TUPLE = (
    ('√', 'x'),
    ('checked', 'unchecked'),
    ('y', 'n'),
    ('yes', 'no'),
    ('enabled', 'disabled'),
    ('on', 'off'),
    ('是', '否'),
    ('完成', '未完成'),
)
CHECKBOX_STRING_LIST = [string for item in CHECKBOX_TUPLE for string in item]
CHECKBOX_TRUE_LIST = [item[0] for item in CHECKBOX_TUPLE]

# copy from dtable-web/frontend/src/components-form/utils/markdown-utils.js
HREF_REG = r'\[.+\]\(\S+\)|<img src=\S+.+\/>|!\[\]\(\S+\)|<\S+>'
LINK_REG_1 = r'^\[.+\]\((\S+)\)'
LINK_REG_2 = r'^<(\S+)>$'
IMAGE_REG_1 = r'^<img src="(\S+)" .+\/>'
IMAGE_REG_2 = r'^!\[\]\((\S+)\)'

UPDATE_TYPE_LIST = ['number', 'single-select', 'url', 'email', 'text', 'date', 'duration', 'rate', 'checkbox',
                    'multiple-select']


class EmptyCell(object):
    value = None


def parse_checkbox(cell_value):
    cell_value = str(cell_value)
    return True if cell_value in CHECKBOX_TRUE_LIST else False


def parse_multiple_select(cell_value):
    cell_value = str(cell_value)
    values = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
    return [value.strip(' ') for value in values]


def parse_image(cell_value):
    cell_value = str(cell_value)
    return cell_value.split(' ')


def parse_number(cell_value):
    try:
        int(cell_value)
    except:
        return ''
    return cell_value


def parse_long_text(cell_value):
    cell_value = str(cell_value)
    checked_count = cell_value.count('[x]')
    unchecked_count = cell_value.count('[ ]')
    total = checked_count + unchecked_count

    href_reg = re.compile(HREF_REG)
    preview = href_reg.sub(' ', cell_value)
    preview = preview[:20].replace('\n', ' ')

    images = []
    links = []
    href_list = href_reg.findall(cell_value)
    for href in href_list:
        if re.search(LINK_REG_1, href):
            links.append(re.search(LINK_REG_1, href).group(1))
        elif re.search(LINK_REG_2, href):
            links.append(re.search(LINK_REG_2, href).group(1))
        elif re.search(IMAGE_REG_1, href):
            images.append(re.search(IMAGE_REG_1, href).group(1))
        elif re.search(IMAGE_REG_2, href):
            images.append(re.search(IMAGE_REG_2, href).group(1))

    return {
        'text': cell_value,
        'preview': preview,
        'checklist': {'completed': checked_count, 'total': total},
        'images': images,
        'links': links,
    }


def parse_excel_rows(sheet_rows, columns, head_index, max_column):
    from dtable_events.dtable_io import dtable_io_logger

    if head_index < 0:
        value_rows = sheet_rows
    else:
        value_rows = sheet_rows[head_index + 1:]
    rows = []
    for row in value_rows:
        row_data = {}
        for index in range(max_column):
            try:
                cell_value = row[index].value
                column_name = columns[index]['name']
                column_type = columns[index]['type']
                if cell_value is None:
                    continue
                if isinstance(cell_value, datetime) or isinstance(cell_value, time):  # JSON serializable
                    cell_value = str(cell_value)

                if column_type == 'number':
                    row_data[column_name] = cell_value
                elif column_type == 'date':
                    row_data[column_name] = str(cell_value)
                elif column_type == 'long-text':
                    row_data[column_name] = parse_long_text(cell_value)
                elif column_type == 'checkbox':
                    row_data[column_name] = parse_checkbox(cell_value)
                elif column_type == 'multiple-select':
                    row_data[column_name] = parse_multiple_select(cell_value)
                else:
                    row_data[column_name] = str(cell_value)
            except Exception as e:
                dtable_io_logger.exception(e)
        if row_data:
            rows.append(row_data)

    return rows


def parse_excel_column_type(value_list):
    from dtable_events.dtable_io import dtable_io_logger

    try:
        type_list = []
        # Check the first 200 rows of data
        for cell_value in value_list[:200]:
            if cell_value is None:
                continue
            elif isinstance(cell_value, int) or isinstance(cell_value, float):
                column_type = 'number'
            elif isinstance(cell_value, datetime):
                column_type = 'date'
            elif isinstance(cell_value, time):
                column_type = 'text'
            elif '\n' in cell_value:
                column_type = 'long-text'
            elif cell_value in CHECKBOX_STRING_LIST:
                column_type = 'checkbox'
            elif (',' in cell_value or '，' in cell_value) \
                    and ('{' not in cell_value):
                column_type = 'multiple-select'
                multiple_value = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
                for value in multiple_value:
                    if len(value.strip(' ')) > 20:
                        # more than 20 characters.
                        column_type = 'text'
            else:
                column_type = 'text'
            type_list.append(column_type)

        max_column_type = max(type_list, key=type_list.count) if type_list else 'text'
        column_data = None
        if max_column_type == 'multiple-select':
            multiple_list = []
            for cell_value in value_list:
                if cell_value is None:
                    continue
                cell_value = str(cell_value)
                multiple_value = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
                for value in multiple_value:
                    value = value.strip(' ')
                    if value not in multiple_list:
                        multiple_list.append(value)
            column_data = {'options': [{'name': value} for value in multiple_list]}

        return max_column_type, column_data
    except Exception as e:
        dtable_io_logger.exception(e)
        return 'text', None


def parse_excel_columns(sheet_rows, head_index, max_column):
    if head_index == -1:
        empty_cell = EmptyCell()
        head_row = [empty_cell] * max_column
        value_rows = sheet_rows
    else:
        head_row = sheet_rows[head_index]
        value_rows = sheet_rows[head_index + 1:]

    columns = []
    for index in range(max_column):
        name = head_row[index].value
        column_name = str(name) if name else 'Field' + str(index + 1)
        value_list = [row[index].value for row in value_rows]
        column_type, column_data = parse_excel_column_type(value_list)
        column = {
            'name': column_name.replace('\ufeff', '').strip(),
            # remove whitespace from both ends of name and BOM char(\ufeff)
            'type': column_type,
            'data': column_data,
        }
        columns.append(column)

    return columns


def parse_excel_to_json(repo_id, dtable_name, custom=False):
    from dtable_events.dtable_io.utils import get_excel_file, \
        upload_excel_json_file, get_excel_json_file
    from dtable_events.dtable_io import dtable_io_logger

    # user custom columns
    if custom:
        json_file = get_excel_json_file(repo_id, dtable_name)
        tables = json.loads(json_file)
        head_index_map = {table['name']: table['head_index'] for table in tables}
    else:
        head_index_map = {}

    # parse
    excel_file = get_excel_file(repo_id, dtable_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True)
    for sheet in wb:
        if sheet.max_row is None or sheet.max_column is None:
            continue
        dtable_io_logger.info(
            'parse sheet: %s, rows: %d, columns: %d' % (sheet.title, sheet.max_row, sheet.max_column))
        try:
            sheet_rows = list(sheet.rows)
        except Exception as e:
            raise Exception('Excel format error')
        max_row = len(sheet_rows)
        max_column = sheet.max_column
        if max_row > 50000:
            max_row = 50000  # rows limit
        if max_column > 500:
            max_column = 500  # columns limit
        if max_row == 0:
            continue

        if custom:
            head_index = head_index_map.get(sheet.title, 0)
            if head_index > max_row - 1:
                head_index = 0
        else:
            head_index = 0

        columns = parse_excel_columns(sheet_rows, head_index, max_column)
        rows = parse_excel_rows(sheet_rows, columns, head_index, max_column)

        dtable_io_logger.info(
            'got table: %s, rows: %d, columns: %d' % (sheet.title, len(rows), len(columns)))

        table = {
            'name': sheet.title,
            'rows': rows,
            'columns': columns,
            'head_index': head_index,
            'max_row': max_row,
            'max_column': max_column,
        }
        tables.append(table)
    wb.close()

    # upload json to file server
    content = json.dumps(tables)
    upload_excel_json_file(repo_id, dtable_name, content)


def import_excel_by_dtable_server(username, repo_id, dtable_uuid, dtable_name):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        upload_excel_json_to_dtable_server, delete_excel_file

    # get json file
    json_file = get_excel_json_file(repo_id, dtable_name)
    # delete excel file
    delete_excel_file(username, repo_id, dtable_name)
    # upload json file to dtable-server
    upload_excel_json_to_dtable_server(username, dtable_uuid, json_file)


def import_excel_add_table_by_dtable_server(username, repo_id, dtable_uuid, dtable_name):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        upload_excel_json_add_table_to_dtable_server, delete_excel_file

    # get json file
    json_file = get_excel_json_file(repo_id, dtable_name)
    # delete excel file
    delete_excel_file(username, repo_id, dtable_name)
    # upload json file to dtable-server
    upload_excel_json_add_table_to_dtable_server(username, dtable_uuid, json_file)


def append_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        append_excel_json_to_dtable_server, delete_excel_file

    # get json file
    json_file = get_excel_json_file(repo_id, file_name)
    # delete excel file
    delete_excel_file(username, repo_id, file_name)
    # upload json file to dtable-server
    append_excel_json_to_dtable_server(username, dtable_uuid, json_file, table_name)


def parse_append_excel_upload_excel_to_json(repo_id, file_name, username, dtable_uuid, table_name):
    from dtable_events.dtable_io.utils import get_excel_file, \
        upload_excel_json_file, get_columns_from_dtable_server
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    excel_file = get_excel_file(repo_id, file_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True)
    sheet = wb.get_sheet_by_name(wb.sheetnames[0])
    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)

    if sheet.max_row is None or sheet.max_column is None:
        wb.close()
        # upload empty json to file server
        table = {
            'name': table_name,
            'rows': [],
            'columns': columns,
            'max_row': 0,
            'max_column': 0,
        }
        tables.append(table)
        content = json.dumps(tables)
        upload_excel_json_file(repo_id, file_name, content)
        return

    dtable_io_logger.info(
        'parse sheet: %s, rows: %d, columns: %d' % (sheet.title, sheet.max_row, sheet.max_column))

    sheet_rows = list(sheet.rows)
    max_row = len(sheet_rows)
    max_column = sheet.max_column
    if max_row > 50000:
        max_row = 50000  # rows limit
    if max_column > 300:
        max_column = 300  # columns limit
    if max_row == 0:
        wb.close()
        # upload empty json to file server
        table = {
            'name': table_name,
            'rows': [],
            'columns': columns,
            'max_row': max_row,
            'max_column': max_column,
        }
        tables.append(table)
        content = json.dumps(tables)
        upload_excel_json_file(repo_id, file_name, content)
        return

    if max_column > len(columns):
        max_column = len(columns)
    rows = parse_append_excel_rows(sheet_rows, columns, len(columns))

    dtable_io_logger.info(
        'got table: %s, rows: %d, columns: %d' % (sheet.title, len(rows), max_column))

    table = {
        'name': table_name,
        'rows': rows,
        'columns': columns,
        'max_row': max_row,
        'max_column': max_column,
    }
    tables.append(table)
    wb.close()

    # upload json to file server
    content = json.dumps(tables)
    upload_excel_json_file(repo_id, file_name, content)


def parse_append_excel_rows(sheet_rows, columns, column_lenght):
    from dtable_events.dtable_io import dtable_io_logger

    value_rows = sheet_rows[1:]
    sheet_head = sheet_rows[0]
    head_dict = {sheet_head[index].value: index for index in range(len(sheet_head))}
    rows = []

    for row in value_rows:
        row_data = {}
        for index in range(column_lenght):
            column_name = columns[index]['name']
            if head_dict.get(column_name) is None:
                continue
            row_index = head_dict.get(column_name)
            try:
                cell_value = row[row_index].value
                column_type = columns[index]['type']
                if cell_value is None:
                    continue
                row_data[column_name] = parse_row(column_type, cell_value)
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        if row_data:
            rows.append(row_data)
    return rows


def get_update_row_data(excel_row, dtable_row, excel_col_name_to_type):
    update_excel_row = {}
    for col_name in excel_col_name_to_type:
        excel_cell_val = excel_row.get(col_name, '')
        dtable_cell_val = dtable_row.get(col_name, '')
        column_type = excel_col_name_to_type.get(col_name)
        if column_type == 'multiple-select':
            if not dtable_cell_val:
                dtable_cell_val = []
            if not excel_cell_val:
                excel_cell_val = []
            excel_cell_val.sort()
            dtable_cell_val.sort()
        elif column_type == 'date' and excel_cell_val:
            # dtable row value like 2021-12-03 00:00 or 2021-12-03, excel row like 2021-12-03 00:00:00
            excel_cell_val = excel_cell_val[0:len(dtable_cell_val)]
        elif column_type == 'checkbox' and not excel_cell_val:
            excel_cell_val = False
        dtable_cell_val = '' if dtable_cell_val is None else dtable_cell_val
        excel_cell_val = '' if excel_cell_val is None else excel_cell_val

        if excel_cell_val != dtable_cell_val:
            update_excel_row[col_name] = excel_cell_val
    if update_excel_row:
        return {'row_id': dtable_row.get('_id'), 'row': update_excel_row}


def get_dtable_row_data(dtable_rows, key_columns):
    dtable_row_data = {}
    for row in dtable_rows:
        key = str(hash('-'.join([str(get_cell_value(row, col)) for col in key_columns])))
        if dtable_row_data.get(key):
            # only deal first row
            continue
        else:
            dtable_row_data[key] = row
    return dtable_row_data


def update_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name, selected_columns):
    from dtable_events.dtable_io.utils import get_excel_json_file, update_rows_by_dtable_server, delete_file, \
        get_rows_from_dtable_server, update_append_excel_json_to_dtable_server, get_columns_from_dtable_server

    # get json file
    json_file = get_excel_json_file(repo_id, file_name)
    sheet_content = json_file.decode()
    excel_rows = json.loads(sheet_content)
    excel_rows = excel_rows[0].get('rows', [])
    dtable_rows = get_rows_from_dtable_server(username, dtable_uuid, table_name)
    key_columns = selected_columns.split(',')

    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)
    dtable_col_name_to_type = {col['name']: col['type'] for col in columns}

    insert_rows, update_rows = get_insert_update_rows(dtable_col_name_to_type, excel_rows, dtable_rows, key_columns)

    # delete excel,json,csv file
    delete_file(username, repo_id, file_name)
    # upload json file to dtable-server
    update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name)
    update_append_excel_json_to_dtable_server(username, dtable_uuid, insert_rows, table_name)


def get_cell_value(row, col):
    cell_value = row.get(col)
    cell_value = '' if cell_value is None else cell_value
    return cell_value


def get_insert_update_rows(dtable_col_name_to_type, excel_rows, dtable_rows, key_columns):
    if not excel_rows:
        return [], []
    update_rows = []
    insert_rows = []
    excel_col_name_to_type = {col_name: dtable_col_name_to_type.get(col_name) for col_name in excel_rows[0].keys()
                             if dtable_col_name_to_type.get(col_name) in UPDATE_TYPE_LIST}

    dtable_row_data = get_dtable_row_data(dtable_rows, key_columns)
    keys_of_excel_rows = {}
    for excel_row in excel_rows:
        excel_row = {col_name: excel_row.get(col_name) for col_name in excel_row if excel_col_name_to_type.get(col_name)}
        key = str(hash('-'.join([str(get_cell_value(excel_row, col)) for col in key_columns])))
        if keys_of_excel_rows.get(key):
            continue
        keys_of_excel_rows[key] = True

        dtable_row = dtable_row_data.get(key)
        if not dtable_row:
            insert_rows.append(excel_row)
        else:
            update_row = get_update_row_data(excel_row, dtable_row, excel_col_name_to_type)
            if update_row:
                update_rows.append(update_row)
    return insert_rows, update_rows


def parse_update_excel_upload_excel_to_json(repo_id, file_name, username, dtable_uuid, table_name):
    from dtable_events.dtable_io.utils import get_excel_file, \
        upload_excel_json_file, get_columns_from_dtable_server
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    excel_file = get_excel_file(repo_id, file_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True)
    sheet = wb.get_sheet_by_name(wb.sheetnames[0])
    dtable_io_logger.info(
        'parse sheet: %s, rows: %d, columns: %d' % (sheet.title, sheet.max_row, sheet.max_column))

    sheet_rows = list(sheet.rows)
    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)
    max_row = len(sheet_rows)
    max_column = sheet.max_column
    if max_row > 50000:
        max_row = 50000  # rows limit
    if max_column > 300:
        max_column = 300  # columns limit
    if max_row == 0:
        wb.close()
        # upload empty json to file server
        table = {
            'name': table_name,
            'rows': [],
            'columns': columns,
            'max_row': max_row,
            'max_column': max_column,
        }
        tables.append(table)
        content = json.dumps(tables)
        upload_excel_json_file(repo_id, file_name, content)
        return

    if max_column > len(columns):
        max_column = len(columns)
    rows = parse_update_excel_rows(sheet_rows, columns, len(columns))

    dtable_io_logger.info(
        'got table: %s, rows: %d, columns: %d' % (sheet.title, len(rows), max_column))

    table = {
        'name': table_name,
        'rows': rows,
        'columns': columns,
        'max_row': max_row,
        'max_column': max_column,
    }
    tables.append(table)
    wb.close()

    # upload json to file server
    content = json.dumps(tables)
    upload_excel_json_file(repo_id, file_name, content)


def parse_update_excel_rows(sheet_rows, columns, column_length):
    from dtable_events.dtable_io import dtable_io_logger

    value_rows = sheet_rows[1:]
    sheet_head = sheet_rows[0]
    head_dict = {sheet_head[index].value: index for index in range(len(sheet_head))}
    rows = []

    for row in value_rows:
        row_data = {}
        for index in range(column_length):
            column_name = columns[index]['name']
            if head_dict.get(column_name) is None:
                continue
            row_index = head_dict.get(column_name)
            try:
                cell_value = row[row_index].value
                column_type = columns[index]['type']
                if cell_value is None:
                    row_data[column_name] = None
                    continue
                row_data[column_name] = parse_row(column_type, cell_value)
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        if row_data:
            rows.append(row_data)
    return rows


def parse_update_csv_upload_csv_to_json(repo_id, file_name, username, dtable_uuid, table_name):
    from dtable_events.dtable_io.utils import get_csv_file, \
        upload_excel_json_file, get_columns_from_dtable_server
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    csv_file = get_csv_file(repo_id, file_name)
    tables = []
    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)

    max_column = 300  # columns limit
    rows, max_column, csv_row_num, csv_column_num = parse_update_csv_rows(csv_file, columns, max_column)
    dtable_io_logger.info(
        'parse csv: %s, rows: %d, columns: %d' % (file_name, csv_row_num, csv_column_num))

    max_row = csv_row_num
    if csv_row_num > 10000:
        max_row = 10000  # rows limit

    dtable_io_logger.info(
        'got table: %s, rows: %d, columns: %d' % (file_name, len(rows), max_column))

    table = {
        'name': table_name,
        'rows': rows,
        'columns': columns,
        'max_row': max_row,
        'max_column': max_column,
    }
    tables.append(table)
    # upload json to file server
    content = json.dumps(tables)
    upload_excel_json_file(repo_id, file_name, content)


def parse_update_csv_rows(csv_file, columns, max_column):
    from dtable_events.dtable_io import dtable_io_logger

    rows = []
    csv_rows = [row for row in csv.reader(csv_file)]
    if not csv_rows:
        return rows, 0, 0, 0

    csv_head = csv_rows[0]
    csv_column_num = len(csv_head)
    table_column_num = len(columns)
    if csv_column_num < max_column:
        max_column = csv_column_num
    if table_column_num > csv_column_num:
        max_column = table_column_num

    csv_head_dict = {csv_head[index].strip(): index for index in range(csv_column_num)}
    csv_row_num = 0
    for csv_row in csv_rows[1:]:
        csv_row_num += 1
        row_data = {}
        for index in range(table_column_num):
            column_name = columns[index]['name']
            row_index = csv_head_dict.get(column_name)
            if row_index is None:
                continue
            try:
                cell_value = csv_row[row_index].strip()
                column_type = columns[index]['type']
                if cell_value is None:
                    row_data[column_name] = None
                    continue
                row_data[column_name] = parse_row(column_type, cell_value)
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        if row_data:
            rows.append(row_data)
    return rows, max_column, csv_row_num, csv_column_num


def parse_row(column_type, cell_value):
    if isinstance(cell_value, datetime):  # JSON serializable
        cell_value = str(cell_value)
    if isinstance(cell_value, str):
        cell_value = cell_value.strip()
    if column_type in ('number', 'duration', 'rate'):
        return parse_number(cell_value)
    elif column_type == 'date':
        return str(cell_value)
    elif column_type == 'long-text':
        return parse_long_text(cell_value)
    elif column_type == 'checkbox':
        return parse_checkbox(cell_value)
    elif column_type == 'multiple-select':
        return parse_multiple_select(cell_value)
    elif column_type in ('url', 'email'):
        return str(cell_value)
    elif column_type == 'text':
        return str(cell_value)
    elif column_type == 'file':
        return None
    elif column_type == 'image':
        return parse_image(cell_value)
    elif column_type == 'single_select':
        return str(cell_value)
    elif column_type == 'link':
        return None
    elif column_type == 'button':
        return None
    elif column_type == 'geolocation':
        return None
    elif column_type in ('collaborator', 'creator', 'last_modifier', 'ctime', 'mtime', 'formula',
                         'link_formula', 'auto_number'):
        return None
    else:
        return str(cell_value)


def get_summary(summary, summary_col_info, column_name):
    summary_type = summary_col_info.get(column_name, 'sum').lower()
    return summary.get(summary_type)


def parse_grouped_rows(grouped_rows, first_col_name, summary_col_info):
    def parse(grouped_rows, sub_level, rows, grouped_row_num_map):
        for group in grouped_rows:
            summaries = group.get('summaries', {})
            grouped_row = {column_name: get_summary(summary, summary_col_info, column_name) for column_name, summary in summaries.items()}
            grouped_row[first_col_name] = group.get('cell_value')
            rows.append(grouped_row)
            grouped_row_num_map[len(rows)] = sub_level

            group_subgroups = group.get('subgroups')
            group_rows = group.get('rows')
            if group_rows is None and group_subgroups:
                parse(group_subgroups, sub_level + 1, rows, grouped_row_num_map)
            else:
                rows.extend(group_rows)

    rows = []
    grouped_row_num_map = {}
    parse(grouped_rows, 0, rows, grouped_row_num_map)
    return rows, grouped_row_num_map


def parse_geolocation(cell_data):
    if not isinstance(cell_data, dict):
        return str(cell_data)
    if 'country_region' in cell_data:
        return cell_data['country_region']
    elif 'lng' in cell_data:
        return str(cell_data['lng']) + ', ' + str(cell_data['lat'])
    elif 'province' in cell_data:
        value = str(cell_data['province'])
        if 'city' in cell_data:
            value = '%s %s' % (value, cell_data['city'])
        if 'district' in cell_data:
            value = '%s %s' % (value, cell_data['district'])
        if 'detail' in cell_data:
            value = '%s %s' % (value, cell_data['detail'])
        return value
    else:
        return str(cell_data)


def parse_link_formula(cell_data, email2nickname):
    from dtable_events.dtable_io import dtable_io_logger
    try:
        # collaborator
        if isinstance(cell_data, list) \
                and isinstance(cell_data[0], str) \
                and VIRTUAL_ID_EMAIL_DOMAIN in cell_data[0]:
            nickname_list = []
            for user in cell_data:
                nickname_list.append(email2nickname.get(user, ''))
            value = ', '.join(nickname_list)
        # ctime, mtime
        elif isinstance(cell_data, str) \
                and '+00:00' in cell_data \
                and 'T' in cell_data:
            utc_time = datetime.strptime(cell_data, '%Y-%m-%dT%H:%M:%S.%f+00:00')
            value = utc_to_tz(utc_time, timezone).strftime('%Y-%m-%d %H:%M:%S')
        # string
        else:
            value = cell_data2str(cell_data)
        return value
    except Exception as e:
        dtable_io_logger.warning(e)
        return cell_data2str(cell_data)


def cell_data2str(cell_data):
    if isinstance(cell_data, list):
        return ' '.join(cell_data2str(item) for item in cell_data)
    else:
        return str(cell_data)


def parse_multiple_select_formula(cell_data):
    if isinstance(cell_data, list):
        return ', '.join(cell_data)
    else:
        return str(cell_data)


def convert_formula_number(value, column_data):
    decimal = column_data.get('decimal')
    thousands = column_data.get('thousands')
    precision = column_data.get('precision')
    if decimal == 'comma':
        # decimal maybe dot or comma
        value = value.replace(',', '.')
    if thousands == 'space':
        # thousands maybe space, dot, comma or no
        value = value.replace(' ', '')
    elif thousands == 'dot':
        value = value.replace('.', '')
        if precision > 0 or decimal == 'dot':
            value = value[:-precision] + '.' + value[-precision:]
    elif thousands == 'comma':
        value = value.replace(',', '')

    return value


def parse_formula_number(cell_data, column_data):
    """
    parse formula number to regular format
    :param cell_data: value of cell (e.g. 1.25, ￥12.0, $10.20, €10.2, 0:02 or 10%, etc)
    :param column_data: info of formula column
    """
    src_format = column_data.get('format')
    value = cell_data
    number_format = '0'
    if src_format in ['euro', 'dollar', 'yuan']:
        value = cell_data[1:]
    elif src_format == 'percent':
        value = cell_data[:-1]
    value = convert_formula_number(value, column_data)

    if src_format == 'number':
        number_format = gen_decimal_format(value)
    elif src_format == 'percent' and isinstance(value, str):
        number_format = gen_decimal_format(value) + '%'
        try:
            value = float(value) / 100
        except Exception as e:
            pass
    elif src_format == 'euro' and isinstance(cell_data, str):
        number_format = '"€"#,##' + gen_decimal_format(value)+'_-'
    elif src_format == 'dollar' and isinstance(cell_data, str):
        number_format = '"$"#,##' + gen_decimal_format(value)+'_-'
    elif src_format == 'yuan' and isinstance(cell_data, str):
        number_format = '"¥"#,##' + gen_decimal_format(value)+'_-'
    try:
        if is_int_str(value):
            value = int(value)
        else:
            value = float(value)
    except Exception as e:
        pass
    return value, number_format


def convert_time_to_utc_str(time_str):
    if 'Z' in time_str:
        utc_time = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    else:
        utc_time = datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S.%f+00:00')
    return utc_to_tz(utc_time, timezone).strftime('%Y-%m-%d %H:%M:%S')


def parse_link(col_head, cell_data, email2nickname):
    if isinstance(cell_data, list):
        if col_head[2].get('array_type') == ColumnTypes.SINGLE_SELECT:
            options = col_head[2].get('array_data', {}).get('options')
            id2name = {op.get('id'): op.get('name') for op in options}
            return ', '.join([id2name.get(cell.get('display_value')) if cell.get('display_value') else '' for cell in cell_data])
        elif col_head[2].get('array_type') in (ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER):
            return ', '.join([email2nickname.get(cell.get('display_value')) if cell.get('display_value') else '' for cell in cell_data])
        elif col_head[2].get('array_type') in (ColumnTypes.CTIME, ColumnTypes.MTIME):
            return ', '.join([convert_time_to_utc_str(cell.get('display_value')) if cell.get('display_value') else '' for cell in cell_data])
        # display_value may be array
        return ', '.join([cell_data2str(cell.get('display_value')) if cell.get('display_value') else '' for cell in cell_data])
    else:
        return str(cell_data)


def is_int_str(num):
    return '.' not in str(num)


def gen_decimal_format(num):
    if is_int_str(num):
        return '0'

    decimal_cnt = len(str(num).split('.')[1])
    if decimal_cnt > 8:
        decimal_cnt = 8
    return '0.' + '0' * decimal_cnt


def _get_strtime_time(time_str):
    from dtable_events.dtable_io import dtable_io_logger
    time_str = time_str.strip()
    if not time_str:
        return ''

    try:
        if ' ' in time_str:
            return datetime.strptime(time_str, '%Y-%m-%d %H:%M')
        else:
            return datetime.strptime(time_str, '%Y-%m-%d')
    except Exception as e:
        dtable_io_logger.debug(e)
    return time_str


def check_and_replace_sheet_name(sheet_name):
    """/ ?\ * [ ] chars is invalid excel sheet name, replace these chars with _ """

    invalid_chars = ['/', '?', '\\', '*', '[', ']', ':']
    for char in invalid_chars:
        if char in sheet_name:
            sheet_name = sheet_name.replace(char, '_')
    return sheet_name


def add_nickname_to_cell(unknown_user_set, unknown_cell_list):
    from dtable_events.dtable_io.utils import get_nicknames_from_dtable

    unknown_user_id_list = list(unknown_user_set)
    step = 1000
    start = 0
    user_list = []
    for i in range(0, len(unknown_user_id_list), step):
        user_list += get_nicknames_from_dtable(unknown_user_id_list[start: start+step])
        start += step

    email2nickname = {nickname['email']: nickname['name'] for nickname in user_list}
    for c in unknown_cell_list:
        if c[2] == ColumnTypes.COLLABORATOR:
            nickname_list, collaborator_email_list = c[1]
            for email in collaborator_email_list:
                nickname_list.append(email2nickname.get(email, ''))
            c[0].value = ', '.join(nickname_list)
        else:
            c[0].value = email2nickname.get(c[1], '')


def handle_row(row, row_num, head, ws, grouped_row_num_map, email2nickname, unknown_user_set, unknown_cell_list):
    for col_num in range(len(row)):
        c = ws.cell(row = row_num + 1, column = col_num + 1)
        if row_num in grouped_row_num_map:
            fill_num = grouped_row_num_map[row_num]
            try:
                c.fill = grouped_row_fills[fill_num]
            except:
                pass

        if not row[col_num] and not isinstance(row[col_num], int) and not isinstance(row[col_num], float):
            continue

        # excel format see
        # https://support.office.com/en-us/article/Number-format-codes-5026bbd6-04bc-48cd-bf33-80f18b4eae68
        if head[col_num][1] == ColumnTypes.NUMBER:
            # if value cannot convert to float or int, just pass, e.g. empty srt ''
            try:
                if is_int_str(row[col_num]):
                    c.value = int(row[col_num])
                else:
                    c.value = float(row[col_num])
            except Exception as e:
                pass
            c.number_format = gen_decimal_format(c.value)
        elif head[col_num][1] == ColumnTypes.DATE:
            c.value = _get_strtime_time(row[col_num])
            if head[col_num][2]:
                c.number_format = head[col_num][2].get('format', '')
            else:
                c.number_format = 'YYYY-MM-DD'
        elif head[col_num][1] in (ColumnTypes.CTIME, ColumnTypes.MTIME):
            if 'Z' in row[col_num]:
                utc_time = datetime.strptime(row[col_num], '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                utc_time = datetime.strptime(row[col_num], '%Y-%m-%dT%H:%M:%S.%f+00:00')
            c.value = utc_to_tz(utc_time, timezone).strftime('%Y-%m-%d %H:%M:%S')
        elif head[col_num][1] == ColumnTypes.GEOLOCATION:
            c.value = parse_geolocation(row[col_num])
        elif head[col_num][1] == ColumnTypes.COLLABORATOR:
            nickname_list = []
            collaborator_email_list = []
            for user in row[col_num]:
                if not email2nickname.get(user, ''):
                    unknown_user_set.add(user)
                    collaborator_email_list.append(user)
                else:
                    nickname_list.append(email2nickname.get(user, ''))
            if collaborator_email_list:
                unknown_cell_list.append((c, (nickname_list, collaborator_email_list), head[col_num][1]))
            c.value = ', '.join(nickname_list)
        elif head[col_num][1] == ColumnTypes.CREATOR:
            if not email2nickname.get(cell_data2str(row[col_num]), ''):
                unknown_user_set.add(cell_data2str(row[col_num]))
                unknown_cell_list.append((c, cell_data2str(row[col_num]), head[col_num][1]))
            c.value = email2nickname.get(cell_data2str(row[col_num]), '')
        elif head[col_num][1] == ColumnTypes.LAST_MODIFIER:
            if not email2nickname.get(cell_data2str(row[col_num]), ''):
                unknown_user_set.add(cell_data2str(row[col_num]))
                unknown_cell_list.append((c, cell_data2str(row[col_num]), head[col_num][1]))
            c.value = email2nickname.get(cell_data2str(row[col_num]), '')
        elif head[col_num][1] == ColumnTypes.LINK_FORMULA:
            c.value = parse_link_formula(row[col_num], email2nickname)
        elif head[col_num][1] == ColumnTypes.MULTIPLE_SELECT:
            c.value = parse_multiple_select_formula(row[col_num])
        elif head[col_num][1] == ColumnTypes.FORMULA \
                and isinstance(head[col_num][2], dict) and head[col_num][2].get('result_type') == 'number':
            c.value, c.number_format = parse_formula_number(row[col_num], head[col_num][2])
        elif head[col_num][1] == ColumnTypes.LINK:
            c.value = parse_link(head[col_num], row[col_num], email2nickname)
        else:
            c.value = cell_data2str(row[col_num])


def write_xls_with_type(sheet_name, head, data_list, grouped_row_num_map, email2nickname):
    """ write listed data into excel
        head is a list of tuples,
        e.g. head = [(col_name, col_type, col_date), (...), ...]
    """
    from dtable_events.dtable_io import dtable_io_logger
    try:
        wb = openpyxl.Workbook()
        ws = wb.active
    except Exception as e:
        dtable_io_logger.error(e)
        return None

    ws.title = check_and_replace_sheet_name(sheet_name)

    row_num = 0

    # write table head
    column_error_log_exists = False
    for col_num in range(len(head)):
        c = ws.cell(row = row_num + 1, column = col_num + 1)
        try:
            c.value = head[col_num][0]
        except Exception as e:
            if not column_error_log_exists:
                dtable_io_logger.error('Error column in exporting excel: {}'.format(e))
                column_error_log_exists = True
            c.value = EXPORT2EXCEL_DEFAULT_STRING

    # write table data
    row_error_log_exists = False
    unknown_user_set = set()
    unknown_cell_list = []
    for row in data_list:
        row_num += 1
        try:
            handle_row(row, row_num, head, ws, grouped_row_num_map, email2nickname, unknown_user_set, unknown_cell_list)
        except Exception as e:
            if not row_error_log_exists:
                dtable_io_logger.error('Error row in exporting excel: {}'.format(e))
                row_error_log_exists = True
            continue
    if unknown_cell_list:
        try:
            add_nickname_to_cell(unknown_user_set, unknown_cell_list)
        except Exception as e:
            dtable_io_logger.error('add nickname to cell error: {}'.format(e))

    return wb
