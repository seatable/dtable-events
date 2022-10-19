import re
import json
import csv
import logging
import os
import sys
import openpyxl
from openpyxl.styles import PatternFill
from openpyxl import load_workbook
from copy import deepcopy
from datetime import datetime, time
from dtable_events.app.config import EXPORT2EXCEL_DEFAULT_STRING, TIME_ZONE
from dtable_events.utils import utc_to_tz
from dtable_events.utils.constants import ColumnTypes

timezone = TIME_ZONE
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
    ('True', 'False'),
    ('true', 'false'),
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
                    'multiple-select', 'collaborator']


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
        return float(cell_value)
    except:
        return ''


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
    """
    parse excel according to excel
    """
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
                cell_value = get_excel_cell_value(row, index)
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


def parse_column_type(value_list):
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

        if max_column_type == 'number' and len(set(type_list)) != 1:
            max_column_type = 'text'

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


def get_excel_cell_value(row, index):
    try:
        return row[index].value
    except:
        return None


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
        name = get_excel_cell_value(head_row, index)
        column_name = str(name) if name else 'Field' + str(index + 1)
        value_list = [get_excel_cell_value(row, index) for row in value_rows]
        column_type, column_data = parse_column_type(value_list)
        column = {
            'name': column_name.replace('\ufeff', '').strip(),
            # remove whitespace from both ends of name and BOM char(\ufeff)
            'type': column_type,
            'data': column_data,
        }
        columns.append(column)

    return columns


def parse_excel(repo_id, dtable_name, custom=False):
    from dtable_events.dtable_io.utils import get_excel_file, get_excel_json_file
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
    wb = load_workbook(excel_file, read_only=True, data_only=True)
    for sheet in wb:
        try:
            sheet_rows = list(sheet.rows)
        except Exception as e:
            raise Exception('Excel format error')

        if not sheet_rows:
            continue

        # the sheet has some rows, but sheet.max_row maybe get None
        max_row = sheet.max_row if isinstance(sheet.max_row, int) else len(sheet_rows)
        max_column = sheet.max_column if isinstance(sheet.max_column, int) else len(sheet_rows[0])
        if not max_row or not max_row:
            continue

        dtable_io_logger.info(
            'parse sheet: %s, rows: %d, columns: %d' % (sheet.title, max_row, max_row))

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

    return json.dumps(tables)


def parse_dtable_csv_columns(sheet_rows, max_column):
    head_row = sheet_rows[0]

    columns = []
    for index in range(max_column):
        name = get_csv_cell_value(head_row, index)
        column_name = str(name) if name else 'Field' + str(index + 1)
        column = {
            'name': column_name.replace('\ufeff', '').strip(),
            'type': 'text'
        }
        columns.append(column)

    return columns


def get_csv_cell_value(row, index):
    try:
        return row[index].strip()
    except:
        return None


def parse_dtable_csv_rows(sheet_rows, columns, max_column):
    from dtable_events.dtable_io import dtable_io_logger

    value_rows = sheet_rows[1:]
    rows = []
    for row in value_rows:
        row_data = {}
        for index in range(max_column):
            try:
                cell_value = get_csv_cell_value(row, index)
                column_name = columns[index]['name']
                column_type = columns[index]['type']
                if cell_value is None:
                    continue
                if column_type == 'long-text':
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


def parse_dtable_csv(repo_id, dtable_name):
    from dtable_events.dtable_io.utils import get_csv_file
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    csv_file = get_csv_file(repo_id, dtable_name)
    tables = []
    delimiter = guess_delimiter(deepcopy(csv_file))
    csv_rows = [row for row in csv.reader(csv_file, delimiter=delimiter)]
    csv_head = csv_rows[0]
    max_row = len(csv_rows)
    max_column = len(csv_head)

    if max_row > 50000:
        max_row = 50000
    if max_column > 500:
        max_column = 500

    columns = parse_dtable_csv_columns(csv_rows, max_column)
    rows = parse_dtable_csv_rows(csv_rows, columns, max_column)

    dtable_io_logger.info(
        'got table: %s, rows: %d, columns: %d' % (dtable_name, len(rows), len(columns)))

    table = {
        'name': dtable_name,
        'rows': rows,
        'columns': columns,
        'max_row': max_row,
        'max_column': max_column,
    }
    tables.append(table)
    return json.dumps(tables)


def parse_and_import_excel_csv_to_dtable(repo_id, dtable_name, dtable_uuid, username, file_type, lang):
    from dtable_events.dtable_io.utils import upload_excel_json_to_dtable_server, delete_file

    if file_type == 'xlsx':
        content = parse_excel(repo_id, dtable_name)
    elif file_type == 'csv':
        content = parse_dtable_csv(repo_id, dtable_name)
    # delete excel、csv、json  file
    delete_file(username, repo_id, dtable_name)
    # import json file to dtable-server
    upload_excel_json_to_dtable_server(username, dtable_uuid, content, lang)


def parse_and_import_excel_csv_to_table(repo_id, file_name, dtable_uuid, username, file_type, lang):
    from dtable_events.dtable_io.utils import upload_excel_json_add_table_to_dtable_server, delete_file

    if file_type == 'xlsx':
        content = parse_excel(repo_id, file_name)
    elif file_type == 'csv':
        content = parse_dtable_csv(repo_id, file_name)
    # delete excel、csv、json  file
    delete_file(username, repo_id, file_name)
    # import json file to dtable-server
    upload_excel_json_add_table_to_dtable_server(username, dtable_uuid, content, lang)


def parse_and_update_file_to_table(repo_id, file_name, username, dtable_uuid, table_name, selected_columns, file_type):
    from dtable_events.dtable_io.utils import update_rows_by_dtable_server, delete_file, \
        get_rows_from_dtable_server, append_rows_by_dtable_server, get_columns_from_dtable_server, \
        get_related_nicknames_from_dtable

    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}

    if file_type == 'xlsx':
        file_rows = parse_dtable_excel_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    else:
        file_rows = parse_csv_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)

    file_rows = file_rows[0].get('rows', [])
    dtable_rows = get_rows_from_dtable_server(username, dtable_uuid, table_name)
    key_columns = selected_columns.split(',')

    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)
    dtable_col_name_to_type = {col['name']: col['type'] for col in columns}

    insert_rows, update_rows = get_insert_update_rows(dtable_col_name_to_type, file_rows, dtable_rows, key_columns)

    # delete excel,json,csv file
    delete_file(username, repo_id, file_name)
    # upload json file to dtable-server
    update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name)
    append_rows_by_dtable_server(username, dtable_uuid, insert_rows, table_name)


def parse_and_append_excel_csv_to_table(username, repo_id, file_name, dtable_uuid, table_name, file_type):
    from dtable_events.dtable_io.utils import append_rows_by_dtable_server, delete_file, get_related_nicknames_from_dtable

    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}

    if file_type == 'xlsx':
        content = parse_dtable_excel_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    elif file_type == 'csv':
        content = parse_csv_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    # delete excel、csv、json  file
    delete_file(username, repo_id, file_name)
    append_rows_by_dtable_server(username, dtable_uuid, content[0]['rows'], table_name)


def parse_excel_csv_to_json(repo_id, dtable_name, file_type, custom=False):
    from dtable_events.dtable_io.utils import upload_excel_json_file
    if file_type == 'xlsx':
        content = parse_excel(repo_id, dtable_name, custom)
    elif file_type == 'csv':
        content = parse_dtable_csv(repo_id, dtable_name)

    upload_excel_json_file(repo_id, dtable_name, content)


def import_excel_csv_by_dtable_server(username, repo_id, dtable_uuid, dtable_name, lang):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        upload_excel_json_to_dtable_server, delete_file

    # get json file
    json_file = get_excel_json_file(repo_id, dtable_name)
    # delete excel、csv、json file
    delete_file(username, repo_id, dtable_name)
    # upload json file to dtable-server
    upload_excel_json_to_dtable_server(username, dtable_uuid, json_file, lang)


def import_excel_csv_add_table_by_dtable_server(username, repo_id, dtable_uuid, dtable_name, lang):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        upload_excel_json_add_table_to_dtable_server, delete_file

    # get json file
    json_file = get_excel_json_file(repo_id, dtable_name)
    # delete excel、csv、json file
    delete_file(username, repo_id, dtable_name)
    # upload json file to dtable-server
    upload_excel_json_add_table_to_dtable_server(username, dtable_uuid, json_file, lang)


def append_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        append_rows_by_dtable_server, delete_file

    # get json file
    json_file = get_excel_json_file(repo_id, file_name)
    # delete excel、csv、json  file
    delete_file(username, repo_id, file_name)
    # upload json file to dtable-server
    rows = json.loads(json_file.decode())[0]['rows']
    append_rows_by_dtable_server(username, dtable_uuid, rows, table_name)


def parse_append_excel_csv_upload_file_to_json(repo_id, file_name, username, dtable_uuid, table_name, file_type):
    from dtable_events.dtable_io.utils import upload_excel_json_file, get_related_nicknames_from_dtable

    # parse
    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}
    if file_type == 'csv':
        tables = parse_csv_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    else:
        tables = parse_dtable_excel_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)

    # upload json to file server
    content = json.dumps(tables)
    upload_excel_json_file(repo_id, file_name, content)


def get_update_row_data(excel_row, dtable_row, excel_col_name_to_type):
    update_excel_row = {}
    for col_name in excel_col_name_to_type:
        excel_cell_val = excel_row.get(col_name, '')
        dtable_cell_val = dtable_row.get(col_name, '')
        column_type = excel_col_name_to_type.get(col_name)
        if column_type == 'multiple-select' or column_type == 'collaborator':
            if not dtable_cell_val:
                dtable_cell_val = []
            if not excel_cell_val:
                excel_cell_val = []
            excel_cell_val.sort()
            dtable_cell_val.sort()
        elif column_type == 'date' and excel_cell_val and dtable_cell_val:
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


def get_dtable_row_data(dtable_rows, key_columns, excel_col_name_to_type):
    dtable_row_data = {}
    for row in dtable_rows:
        key = str(hash('-'.join([str(get_cell_value(row, col, excel_col_name_to_type)) for col in key_columns])))
        if dtable_row_data.get(key):
            # only deal first row
            continue
        else:
            dtable_row_data[key] = row
    return dtable_row_data


def update_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name, selected_columns):
    from dtable_events.dtable_io.utils import get_excel_json_file, update_rows_by_dtable_server, delete_file, \
        get_rows_from_dtable_server, append_rows_by_dtable_server, get_columns_from_dtable_server

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
    append_rows_by_dtable_server(username, dtable_uuid, insert_rows, table_name)


def get_cell_value(row, col, excel_col_name_to_type):
    cell_value = row.get(col)
    col_type = excel_col_name_to_type.get(col)
    if col_type == 'number':
        if isinstance(cell_value, float):
            cell_value = str(cell_value).rstrip('0')
            cell_value = int(cell_value.rstrip('.')) if cell_value.endswith('.') else float(cell_value)

    cell_value = '' if cell_value is None else cell_value
    return cell_value


def get_insert_update_rows(dtable_col_name_to_type, excel_rows, dtable_rows, key_columns):
    if not excel_rows:
        return [], []
    update_rows = []
    insert_rows = []
    excel_col_name_to_type = {col_name: dtable_col_name_to_type.get(col_name) for col_name in excel_rows[0].keys()
                              if dtable_col_name_to_type.get(col_name) in UPDATE_TYPE_LIST}

    dtable_row_data = get_dtable_row_data(dtable_rows, key_columns, excel_col_name_to_type)
    keys_of_excel_rows = {}
    for excel_row in excel_rows:
        excel_row = {col_name: excel_row.get(col_name) for col_name in excel_row if excel_col_name_to_type.get(col_name)}
        key = str(hash('-'.join([str(get_cell_value(excel_row, col, excel_col_name_to_type)) for col in key_columns])))
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


def parse_dtable_excel_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email):
    from dtable_events.dtable_io.utils import get_excel_file, get_columns_from_dtable_server
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    excel_file = get_excel_file(repo_id, file_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True, data_only=True)
    sheet = wb.get_sheet_by_name(wb.sheetnames[0])

    sheet_rows = list(sheet.rows)
    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)
    if not sheet_rows:
        wb.close()
        table = {
            'name': table_name,
            'rows': [],
            'columns': columns,
            'max_row': 0,
            'max_column': 0,
        }
        tables.append(table)
        return tables

    # the sheet has some rows, but sheet.max_row maybe get None
    max_row = sheet.max_row if isinstance(sheet.max_row, int) else len(sheet_rows)
    max_column = sheet.max_column if isinstance(sheet.max_column, int) else len(sheet_rows[0])

    dtable_io_logger.info(
        'parse sheet: %s, rows: %d, columns: %d' % (sheet.title, max_row, max_column))

    if max_row > 50000:
        max_row = 50000  # rows limit
    if max_column > 500:
        max_column = 500  # columns limit

    if max_column > len(columns):
        max_column = len(columns)
    rows = parse_dtable_excel_rows(sheet_rows, columns, len(columns), name_to_email)

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

    return tables


def parse_update_excel_upload_excel_to_json(repo_id, file_name, username, dtable_uuid, table_name):
    from dtable_events.dtable_io.utils import upload_excel_json_file, get_related_nicknames_from_dtable

    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}

    content = parse_dtable_excel_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    upload_excel_json_file(repo_id, file_name, json.dumps(content))


def parse_dtable_excel_rows(sheet_rows, columns, column_length, name_to_email):
    """
    parse excel according to dtable
    """
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
                cell_value = get_excel_cell_value(row, row_index)
                column_type = columns[index]['type']
                if cell_value is None:
                    row_data[column_name] = None
                    continue
                row_data[column_name] = parse_row(column_type, cell_value, name_to_email)
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        if row_data:
            rows.append(row_data)
    return rows


def parse_csv_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email):
    from dtable_events.dtable_io.utils import get_csv_file, get_columns_from_dtable_server
    from dtable_events.dtable_io import dtable_io_logger

    # parse
    csv_file = get_csv_file(repo_id, file_name)
    tables = []
    columns = get_columns_from_dtable_server(username, dtable_uuid, table_name)

    max_column = 500  # columns limit
    rows, max_column, csv_row_num, csv_column_num = parse_csv_rows(csv_file, columns, max_column, name_to_email)
    dtable_io_logger.info(
        'parse csv: %s, rows: %d, columns: %d' % (file_name, csv_row_num, csv_column_num))

    max_row = csv_row_num
    if csv_row_num > 50000:
        max_row = 50000  # rows limit

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
    return tables


def parse_update_csv_upload_csv_to_json(repo_id, file_name, username, dtable_uuid, table_name):
    from dtable_events.dtable_io.utils import upload_excel_json_file, get_related_nicknames_from_dtable

    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}
    content = parse_csv_file(repo_id, file_name, username, dtable_uuid, table_name, name_to_email)
    upload_excel_json_file(repo_id, file_name, json.dumps(content))


def guess_delimiter(csv_file):
    line = csv_file.readline()

    if not line:
        return ','
    comma_count = line.count(',')
    semicolon_count = line.count(';')
    delimiter = comma_count >= semicolon_count and ',' or ';'

    return delimiter


def parse_csv_rows(csv_file, columns, max_column, name_to_email):
    from dtable_events.dtable_io import dtable_io_logger

    rows = []
    delimiter = guess_delimiter(deepcopy(csv_file))
    csv_rows = [row for row in csv.reader(csv_file, delimiter=delimiter)]

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
                cell_value = get_csv_cell_value(csv_row, row_index)
                column_type = columns[index]['type']
                if cell_value is None:
                    row_data[column_name] = None
                    continue
                parsed_value = parse_row(column_type, cell_value, name_to_email)
                row_data[column_name] = parsed_value
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        if row_data:
            rows.append(row_data)
    return rows, max_column, csv_row_num, csv_column_num


def parse_row(column_type, cell_value, name_to_email):
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
    elif column_type == 'single-select':
        return str(cell_value)
    elif column_type == 'link':
        return None
    elif column_type == 'button':
        return None
    elif column_type == 'geolocation':
        return None
    elif column_type in ('creator', 'last-modifier', 'ctime', 'mtime', 'formula', 'link-formula', 'auto-number'):
        return None
    elif column_type == 'collaborator':
        cell_value = parse_collaborator(cell_value, name_to_email)
        return cell_value
    else:
        return str(cell_value)


def parse_collaborator(cell_value, name_to_email):
    if not isinstance(cell_value, str):
        return []
    users = re.split('[,，]', cell_value)
    email_list = []
    for user in users:
        email = name_to_email.get(user.strip())
        if email:
            email_list.append(email)
    return email_list


def get_summary(summary, summary_col_info, column_name, head_name_to_head):
    summary_type = summary_col_info.get(column_name, 'sum').lower()
    column_info = head_name_to_head.get(column_name)
    # return summary info if column type is formula and result type is number for excel value
    # because grouped summary row does not contain format symbol like $, ￥, %, etc
    if column_info and column_info[1] == 'formula' and column_info[2].get('result_type') == 'number':
        return parse_summary_value(summary.get(summary_type), column_info[2])
    return summary.get(summary_type)


def parse_grouped_rows(grouped_rows, first_col_name, summary_col_info, head_name_to_head):
    def parse(grouped_rows, sub_level, rows, grouped_row_num_map):
        for group in grouped_rows:
            summaries = group.get('summaries', {})
            grouped_row = {column_name: get_summary(summary, summary_col_info, column_name, head_name_to_head) for column_name, summary in summaries.items()}
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
            value = '%s%s' % (value, cell_data['city'])
        if 'district' in cell_data:
            value = '%s%s' % (value, cell_data['district'])
        if 'detail' in cell_data:
            value = '%s%s' % (value, cell_data['detail'])
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


def parse_summary_value(cell_data, column_data):
    value = str(cell_data)
    precision = column_data.get('precision', 0)
    src_format = column_data.get('format')

    if src_format == 'percent':
        try:
            if is_int_str(value):
                value = str(int(value) * 100)
            else:
                value = str(float(value) * 100)
        except:
            pass
    elif src_format == 'duration':
        duration_format = column_data.get('duration_format', 'h:mm')
        duration_value = float(value)
        h_value = str(duration_value // 3600).split('.')[0]
        m_value = str((duration_value % 3600) // 60).split('.')[0]
        s_value = str(duration_value % 60).split('.')[0]
        if len(m_value) == 1:
            m_value = '0' + m_value
        if duration_format == 'h:mm':

            return h_value + ':' + m_value
        else:
            if len(s_value) == 1:
                s_value = '0' + s_value
            return h_value + ':' + m_value + ':' + s_value
    value_list = value.split('.')
    value_precision = len(value_list[1]) if (len(value_list) > 1) else 0

    if precision > 0 and precision > value_precision:
        if value_precision > 0:
            value = value + '0' * (precision - value_precision)
        else:
            value = value + '.' + '0' * (precision - value_precision)

    # add symbol
    if src_format == 'euro':
        value = '€' + value
    elif src_format == 'dollar':
        value = '$' + value
    elif src_format == 'yuan':
        value = '￥' + value
    elif src_format == 'percent':
        value = value + '%'
    elif src_format == 'custom_currency':
        currency_symbol = column_data.get('currency_symbol')
        currency_symbol_position = column_data.get('currency_symbol_position', 'before')
        if currency_symbol_position == 'before':
            value = currency_symbol + value
        else:
            value = value + currency_symbol
    return value


def parse_formula_number(cell_data, column_data):
    """
    parse formula number to regular format
    :param cell_data: value of cell (e.g. 1.25, ￥12.0, $10.20, €10.2, 0:02 or 10%, etc)
    :param column_data: info of formula column
    """
    src_format = column_data.get('format')
    value = str(cell_data)
    if src_format in ['euro', 'dollar', 'yuan']:
        value = value[1:]
    elif src_format == 'percent':
        value = value[:-1]
    elif src_format == 'custom_currency':
        currency_symbol = column_data.get('currency_symbol')
        currency_symbol_position = column_data.get('currency_symbol_position', 'before')
        if currency_symbol_position == 'before':
            value = value[len(currency_symbol):]
        else:
            value = value[:-len(currency_symbol)]
    value = convert_formula_number(value, column_data)

    number_format = '0'
    if src_format == 'number':
        number_format = gen_decimal_format(value)
    elif src_format == 'percent' and isinstance(value, str):
        number_format = gen_decimal_format(value) + '%'
        try:
            value = float(value) / 100
        except Exception as e:
            pass
    elif src_format == 'euro':
        number_format = '"€"#,##' + gen_decimal_format(value)+'_-'
    elif src_format == 'dollar':
        number_format = '"$"#,##' + gen_decimal_format(value)+'_-'
    elif src_format == 'yuan':
        number_format = '"¥"#,##' + gen_decimal_format(value)+'_-'
    elif src_format == 'custom_currency':
        currency_symbol = column_data.get('currency_symbol')
        currency_symbol_position = column_data.get('currency_symbol_position', 'before')
        if currency_symbol_position == 'before':
            number_format = '"%s"#,##' % currency_symbol + gen_decimal_format(value) + '_-'
        else:
            number_format = gen_decimal_format(value) + currency_symbol

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

    try:
        time_str = time_str.strip()
    except:
        return ''

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


def parse_dtable_long_text(cell_value):
    if not isinstance(cell_value, str):
        return ''
    if cell_value.find('\n\n') == -1:
        return cell_value
    return parse_dtable_long_text(cell_value.replace('\n\n', '\n'))


def handle_row(row, row_num, head, ws, grouped_row_num_map, email2nickname, unknown_user_set, unknown_cell_list):
    from openpyxl.cell import WriteOnlyCell
    cell_list = []
    for col_num in range(len(row)):

        if not row[col_num] and not isinstance(row[col_num], int) and not isinstance(row[col_num], float):
            c = WriteOnlyCell(ws, value=None)

        # excel format see
        # https://support.office.com/en-us/article/Number-format-codes-5026bbd6-04bc-48cd-bf33-80f18b4eae68
        elif head[col_num][1] == ColumnTypes.NUMBER:
            # if value cannot convert to float or int, just pass, e.g. empty srt ''
            try:
                if is_int_str(row[col_num]):
                    c = WriteOnlyCell(ws, value=int(row[col_num]))
                else:
                    c = WriteOnlyCell(ws, value=float(row[col_num]))
            except Exception as e:
                c = WriteOnlyCell(ws, value=None)
            else:
                c.number_format = gen_decimal_format(row[col_num])
        elif head[col_num][1] == ColumnTypes.DATE:
            c = WriteOnlyCell(ws, value=_get_strtime_time(row[col_num]))
            if head[col_num][2]:
                c.number_format = head[col_num][2].get('format', '')
            else:
                c.number_format = 'YYYY-MM-DD'
        elif head[col_num][1] in (ColumnTypes.CTIME, ColumnTypes.MTIME):
            if 'Z' in row[col_num]:
                utc_time = datetime.strptime(row[col_num], '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                utc_time = datetime.strptime(row[col_num], '%Y-%m-%dT%H:%M:%S.%f+00:00')
            c = WriteOnlyCell(ws, value=utc_to_tz(utc_time, timezone).strftime('%Y-%m-%d %H:%M:%S'))
        elif head[col_num][1] == ColumnTypes.GEOLOCATION:
            c = WriteOnlyCell(ws, value=parse_geolocation(row[col_num]))
        elif head[col_num][1] == ColumnTypes.COLLABORATOR:
            nickname_list = []
            collaborator_email_list = []
            for user in row[col_num]:
                if not email2nickname.get(user, ''):
                    unknown_user_set.add(user)
                    collaborator_email_list.append(user)
                else:
                    nickname_list.append(email2nickname.get(user, ''))
            nicknames = ', '.join(nickname_list)
            c = WriteOnlyCell(ws, value=nicknames)
            if collaborator_email_list:
                unknown_cell_list.append((c, (nickname_list, collaborator_email_list), head[col_num][1]))
            c.value = ', '.join(nickname_list)
        elif head[col_num][1] == ColumnTypes.CREATOR:
            c = WriteOnlyCell(ws, value=email2nickname.get(cell_data2str(row[col_num]), ''))
            if not email2nickname.get(cell_data2str(row[col_num]), ''):
                unknown_user_set.add(cell_data2str(row[col_num]))
                unknown_cell_list.append((c, cell_data2str(row[col_num]), head[col_num][1]))
        elif head[col_num][1] == ColumnTypes.LAST_MODIFIER:
            c = WriteOnlyCell(ws, value=email2nickname.get(cell_data2str(row[col_num]), ''))
            if not email2nickname.get(cell_data2str(row[col_num]), ''):
                unknown_user_set.add(cell_data2str(row[col_num]))
                unknown_cell_list.append((c, cell_data2str(row[col_num]), head[col_num][1]))
        elif head[col_num][1] == ColumnTypes.LINK_FORMULA:
            c = WriteOnlyCell(ws, value=parse_link_formula(row[col_num], email2nickname))
        elif head[col_num][1] == ColumnTypes.MULTIPLE_SELECT:
            c = WriteOnlyCell(ws, value=parse_multiple_select_formula(row[col_num]))
        elif head[col_num][1] == ColumnTypes.FORMULA \
                and isinstance(head[col_num][2], dict) and head[col_num][2].get('result_type') == 'number':
            formula_value, number_format = parse_formula_number(row[col_num], head[col_num][2])
            c = WriteOnlyCell(ws, value=formula_value)
            c.number_format = number_format
        elif head[col_num][1] == ColumnTypes.LINK:
            c = WriteOnlyCell(ws, value=parse_link(head[col_num], row[col_num], email2nickname))
        elif head[col_num][1] == ColumnTypes.LONG_TEXT:
            c = WriteOnlyCell(ws, value=parse_dtable_long_text(row[col_num]))
        else:
            c = WriteOnlyCell(ws, value=cell_data2str(row[col_num]))
        if row_num in grouped_row_num_map:
            fill_num = grouped_row_num_map[row_num]
            try:
                c.fill = grouped_row_fills[fill_num]
            except:
                pass
        cell_list.append(c)
    return cell_list


def write_xls_with_type(head, data_list, grouped_row_num_map, email2nickname, ws, row_num):
    """ write listed data into excel
        head is a list of tuples,
        e.g. head = [(col_name, col_type, col_date), (...), ...]
    """
    from dtable_events.dtable_io import dtable_io_logger
    from openpyxl.cell import WriteOnlyCell

    if row_num == 0:
        # write table head
        column_error_log_exists = False
        head_cell_list = []
        for col_num in range(len(head)):
            try:
                c = WriteOnlyCell(ws, value=head[col_num][0])
            except Exception as e:
                if not column_error_log_exists:
                    dtable_io_logger.error('Error column in exporting excel: {}'.format(e))
                    column_error_log_exists = True
                c = WriteOnlyCell(ws, value=EXPORT2EXCEL_DEFAULT_STRING)
            head_cell_list.append(c)
        ws.append(head_cell_list)

    # write table data
    row_error_log_exists = False
    unknown_user_set = set()
    unknown_cell_list = []
    row_list = []
    for row in data_list:
        row_num += 1
        try:
            row_cells = handle_row(row, row_num, head, ws, grouped_row_num_map, email2nickname, unknown_user_set, unknown_cell_list)
        except Exception as e:
            if not row_error_log_exists:
                dtable_io_logger.error('Error row in exporting excel: {}'.format(e))
                row_error_log_exists = True
            continue
        row_list.append(row_cells)

    if unknown_cell_list:
        try:
            add_nickname_to_cell(unknown_user_set, unknown_cell_list)
        except Exception as e:
            dtable_io_logger.error('add nickname to cell error: {}'.format(e))
    for row in row_list:
        ws.append(row)
