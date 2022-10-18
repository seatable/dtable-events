import openpyxl
import os

from dtable_events.dtable_io.excel import parse_row
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.app.config import INNER_DTABLE_DB_URL
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.dtable_server_api import DTableServerAPI

AUTO_GENERATED_COLUMNS = [
    ColumnTypes.AUTO_NUMBER,
    ColumnTypes.CTIME,
    ColumnTypes.MTIME,
    ColumnTypes.CREATOR,
    ColumnTypes.LAST_MODIFIER,
    ColumnTypes.BUTTON,
    ColumnTypes.FORMULA,
    ColumnTypes.LINK_FORMULA,
]

ROW_EXCEED_ERROR_CODE = 1
FILE_READ_ERROR_CODE = 2
COLUMN_MATCH_ERROR_CODE = 3
ROW_INSERT_ERROR_CODE = 4
INTERNAL_ERROR_CODE = 5

def match_columns(authed_base, table_name, target_columns):
    table_columns = authed_base.list_columns(table_name)
    for col in table_columns:
        col_type = col.get('type')
        if col_type in AUTO_GENERATED_COLUMNS:
            continue
        col_name = col.get('name')
        if col_name not in target_columns:
            return False, col_name, table_columns

    return True, None, table_columns

def import_excel_to_db(
        username,
        dtable_uuid,
        table_name,
        file_path,
        task_id,
        tasks_status_map,

):
    from dtable_events.dtable_io import dtable_io_logger

    tasks_status_map[task_id] = {
        'status': 'initializing',
        'err_msg': '',
        'rows_imported': 0,
        'total_rows': 0,
        'err_code': 0,
    }
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        sheets = wb.get_sheet_names()
        ws = wb[sheets[0]]
        total_rows = ws.max_row and ws.max_row - 1 or 0
        if total_rows > 1000000:
            tasks_status_map[task_id]['err_msg'] = 'Number of rows (%s) exceeds 100,000 limit' % total_rows
            tasks_status_map[task_id]['status'] = 'terminated'
            tasks_status_map[task_id]['err_code'] = ROW_EXCEED_ERROR_CODE
            os.remove(file_path)
            return
    except Exception as err:
        tasks_status_map[task_id]['err_msg'] = "file reading error: %s" % str(err)
        tasks_status_map[task_id]['status'] = 'terminated'
        tasks_status_map[task_id]['err_code'] = FILE_READ_ERROR_CODE
        os.remove(file_path)
        return

    try:

        dtable_server_url = get_inner_dtable_server_url()
        excel_columns = [cell.value for cell in ws[1]]

        base = DTableServerAPI(username, dtable_uuid, dtable_server_url)
        column_matched, column_name, base_columns = match_columns(base, table_name, excel_columns)
        if not column_matched:
            tasks_status_map[task_id]['err_msg'] = 'Column %s does not match in excel' % column_name
            tasks_status_map[task_id]['status'] = 'terminated'
            tasks_status_map[task_id]['err_code'] = COLUMN_MATCH_ERROR_CODE
            os.remove(file_path)
            return

        db_handler = DTableDBAPI(username, dtable_uuid, INNER_DTABLE_DB_URL)
    except Exception as err:
        tasks_status_map[task_id]['err_msg'] = str(err)
        tasks_status_map[task_id]['status'] = 'terminated'
        tasks_status_map[task_id]['err_code'] = INTERNAL_ERROR_CODE
        os.remove(file_path)
        return

    total_count = 0
    insert_count = 0
    slice = []


    status = 'success'
    tasks_status_map[task_id]['status'] = 'running'
    tasks_status_map[task_id]['total_rows'] = total_rows

    column_name_type_map = {col.get('name'): col.get('type') for col in base_columns}
    index = 0
    for row in ws.rows:
        try:
            if index > 0:
                row_list = [r.value for r in row]
                row_data = dict(zip(excel_columns, row_list))
                parsed_row_data = {}
                for col_name, value in row_data.items():
                    col_type = column_name_type_map.get(col_name)
                    parsed_row_data[col_name] = value and parse_row(col_type, value, None) or ''
                slice.append(parsed_row_data)
                if total_count + 1 == total_rows or len(slice) == 100:
                    tasks_status_map[task_id]['rows_imported'] = insert_count
                    db_handler.insert_rows(table_name, slice)
                    insert_count += len(slice)
                    slice = []
                total_count += 1
            index += 1
        except Exception as err:
            tasks_status_map[task_id]['err_msg'] = 'Row inserted error'
            tasks_status_map[task_id]['status'] = 'terminated'
            tasks_status_map[task_id]['err_code'] = ROW_INSERT_ERROR_CODE
            dtable_io_logger.error(str(err))
            os.remove(file_path)
            return

    tasks_status_map[task_id]['status'] = status
    tasks_status_map[task_id]['rows_imported'] = insert_count
    os.remove(file_path)
    return
