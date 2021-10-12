import re
import json
from datetime import datetime

from openpyxl import load_workbook
import csv

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


class EmptyCell(object):
    value = None


def parse_checkbox(cell_value):
    cell_value = str(cell_value)
    return True if cell_value in CHECKBOX_TRUE_LIST else False


def parse_multiple_select(cell_value):
    cell_value = str(cell_value)
    values = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
    return [value.strip(' ') for value in values]


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
                if isinstance(cell_value, datetime):  # JSON serializable
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
    # for sheet in wb:
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
    rows = parse_append_excel_rows(sheet_rows, columns, max_column)

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


def parse_append_excel_rows(sheet_rows, columns, max_column):
    from dtable_events.dtable_io import dtable_io_logger

    value_rows = sheet_rows[1:]
    sheet_head = sheet_rows[0]
    head_dict = {sheet_head[index].value: index for index in range(len(sheet_head))}
    rows = []

    for row in value_rows:
        row_data = {}
        for index in range(max_column):
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
        rows.append(row_data)
    return rows


def get_insert_update_result(excel_row, dtable_rows, selected_column_list):
    update_row_list = []
    for dtable_row in dtable_rows:
        is_update = True
        for selected_column in selected_column_list:
            dtable_cell_value = dtable_row.get(selected_column)
            excel_cell_value = excel_row.get(selected_column)
            if dtable_cell_value != excel_cell_value:
                is_update = False
                break
        if is_update:
            update_row = {'row_id': dtable_row.get('_id'), 'row': excel_row}
            update_row_list.append(update_row)
    if update_row_list:
        return update_row_list, 'update'
    return excel_row, 'insert'


def update_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name, selected_columns):
    from dtable_events.dtable_io.utils import get_excel_json_file, update_rows_by_dtable_server, \
        delete_file, get_rows_from_dtable_server, update_append_excel_json_to_dtable_server

    # get json file
    json_file = get_excel_json_file(repo_id, file_name)
    sheet_content = json_file.decode()
    excel_rows = json.loads(sheet_content)
    excel_rows = excel_rows[0].get('rows', [])
    dtable_rows = get_rows_from_dtable_server(username, dtable_uuid, table_name)
    selected_column_list = selected_columns.split(',')
    update_rows = []
    insert_rows = []
    for excel_row in excel_rows:
        rows, operateType = get_insert_update_result(excel_row, dtable_rows, selected_column_list)
        if operateType == 'insert':
            insert_rows.append(rows)
        else:
            update_rows += rows

    # delete excel,json,csv file
    delete_file(username, repo_id, file_name)
    # upload json file to dtable-server
    update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name)
    update_append_excel_json_to_dtable_server(username, dtable_uuid, insert_rows, table_name)


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
    rows = parse_update_excel_rows(sheet_rows, columns, max_column)

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


def parse_update_excel_rows(sheet_rows, columns, max_column):
    from dtable_events.dtable_io import dtable_io_logger

    value_rows = sheet_rows[1:]
    sheet_head = sheet_rows[0]
    head_dict = {sheet_head[index].value: index for index in range(len(sheet_head))}
    rows = []

    for row in value_rows:
        row_data = {}
        for index in range(max_column):
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
        for index in range(max_column):
            column_name = columns[index]['name']
            if csv_head_dict.get(column_name) is None:
                continue
            row_index = csv_head_dict.get(column_name)
            try:
                cell_value = csv_row[row_index].strip()
                column_type = columns[index]['type']
                if cell_value is None:
                    continue
                row_data[column_name] = parse_row(column_type, cell_value)
            except Exception as e:
                dtable_io_logger.exception(e)
                row_data[column_name] = None
        rows.append(row_data)
    return rows, max_column, csv_row_num, csv_column_num


def parse_row(column_type, cell_value):
    if isinstance(cell_value, datetime):  # JSON serializable
        cell_value = str(cell_value)
    if column_type in ('number', 'duration', 'rating'):
        return parse_number(cell_value)
    elif column_type == 'date':
        return str(cell_value)
    elif column_type == 'long-text':
        return parse_long_text(cell_value)
    elif column_type == 'checkbox':
        return parse_checkbox(cell_value)
    elif column_type == 'multi-select':
        return parse_multiple_select(cell_value)
    elif column_type in ('URL', 'email'):
        return str(cell_value)
    elif column_type == 'text':
        return str(cell_value)
    elif column_type == 'file':
        return None
    elif column_type == 'image':
        return str(cell_value)
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
