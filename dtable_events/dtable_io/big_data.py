import openpyxl
import os
import json

from dtable_events.dtable_io.excel import parse_row
from dtable_events.dtable_io.utils import get_related_nicknames_from_dtable
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.app.config import INNER_DTABLE_DB_URL, dtable_web_dir
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

def get_location_tree_json():

    json_path = os.path.join(dtable_web_dir, 'media/geo-data/cn-location.json')

    with open(json_path, 'r', encoding='utf8') as fp:
        json_data = json.load(fp)

    return json_data



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


def query_row_ids_with_ref_columns(db_api, table_name, row_data, ref_cols, column_name_type_map):
    where_clauses = []
    for ref_col in ref_cols:
        col_type = column_name_type_map.get(ref_col)
        value = row_data.get(ref_col)
        if not value:
            where_clauses.append(
                "`%s` is null" % ref_col
            )
        else:
            if col_type == ColumnTypes.NUMBER:
                where_clauses.append(
                    "`%s`=%s" % (ref_col, value)
                )
            else:
                where_clauses.append(
                    "`%s`='%s'" %(ref_col, value)
                )
    sql = "Select _id from `%s` where %s" % (
        table_name,
        ' And '.join(where_clauses)
    )

    res = db_api.query(sql)
    return res and [r.get('_id') for r in res] or []


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
        if total_rows > 500000:
            tasks_status_map[task_id]['err_msg'] = 'Number of rows (%s) exceeds 500,000 limit' % total_rows
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
    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}

    location_tree = get_location_tree_json()

    index = 0
    for row in ws.rows:
        try:
            if index > 0:
                row_list = [r.value for r in row]
                row_data = dict(zip(excel_columns, row_list))
                parsed_row_data = {}
                for col_name, value in row_data.items():
                    col_type = column_name_type_map.get(col_name)
                    parsed_row_data[col_name] = value and parse_row(col_type, value, name_to_email, location_tree=location_tree) or ''
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

def update_excel_to_db(
        username,
        dtable_uuid,
        table_name,
        file_path,
        ref_columns,
        is_insert_new_data,
        task_id,
        tasks_status_map,

):
    from dtable_events.dtable_io import dtable_io_logger

    tasks_status_map[task_id] = {
        'status': 'initializing',
        'err_msg': '',
        'rows_handled': 0,
        'total_rows': 0,
        'err_code': 0,
    }
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        sheets = wb.get_sheet_names()
        ws = wb[sheets[0]]
        total_rows = ws.max_row and ws.max_row - 1 or 0
        if total_rows > 500000:
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
    ref_columns = ref_columns.split(',')
    try:
        dtable_server_url = get_inner_dtable_server_url()
        excel_columns = [cell.value for cell in ws[1]]
        for ref_col  in ref_columns:
            if ref_col not in excel_columns:
                tasks_status_map[task_id]['err_msg'] = 'Column %s does not exist in excel' % ref_col
                tasks_status_map[task_id]['status'] = 'terminated'
                tasks_status_map[task_id]['err_code'] = COLUMN_MATCH_ERROR_CODE
                os.remove(file_path)
                return

        db_handler = DTableDBAPI(username, dtable_uuid, INNER_DTABLE_DB_URL)
        base = DTableServerAPI(username, dtable_uuid, dtable_server_url)
        column_name_type_map = {col.get('name'): col.get('type') for col in base.list_columns(table_name)}
    except Exception as err:
        tasks_status_map[task_id]['err_msg'] = str(err)
        tasks_status_map[task_id]['status'] = 'terminated'
        tasks_status_map[task_id]['err_code'] = INTERNAL_ERROR_CODE
        os.remove(file_path)
        return

    total_count = 0  # data in excel scanned

    update_rows = []
    import_rows = []

    related_users = get_related_nicknames_from_dtable(dtable_uuid, username, 'r')
    name_to_email = {user.get('name'): user.get('email') for user in related_users}

    location_tree = get_location_tree_json()

    index = 0
    status = 'success'
    tasks_status_map[task_id]['status'] = 'running'
    tasks_status_map[task_id]['total_rows'] = total_rows
    for row in ws.rows:
        try:
            if index > 0:
                row_list = [r.value for r in row]
                row_data = dict(zip(excel_columns, row_list))
                row_ids = query_row_ids_with_ref_columns(
                    db_handler,table_name, row_data, ref_columns,column_name_type_map
                )
                # 1. for import
                if not row_ids:
                    if is_insert_new_data:
                        parsed_row_data = {}
                        for col_name, value in row_data.items():
                            col_type = column_name_type_map.get(col_name)
                            if col_type in AUTO_GENERATED_COLUMNS:
                                continue
                            parsed_row_data[col_name] = value and parse_row(col_type, value, name_to_email, location_tree=location_tree) or ''
                        import_rows.append(parsed_row_data)

                # 2. for update
                else:
                    updates = []
                    parsed_row_data = {}
                    for col_name, value in row_data.items():
                        col_type = column_name_type_map.get(col_name)
                        if col_type in AUTO_GENERATED_COLUMNS:
                            continue
                        parsed_row_data[col_name] = value and parse_row(col_type, value, name_to_email, location_tree=location_tree) or ''
                    for row_id in row_ids:
                        updates.append({
                            'row_id': row_id,
                            'row': parsed_row_data or {}
                        })
                        update_rows.extend(updates)


                if total_count + 1 >= total_rows or len(update_rows) >= 100:
                    tasks_status_map[task_id]['rows_handled'] = total_count
                    db_handler.batch_update_rows(table_name, update_rows)
                    update_rows = []
                if total_count + 1 >= total_rows or len(import_rows) >= 100:
                    tasks_status_map[task_id]['rows_handled'] = total_count
                    db_handler.insert_rows(table_name, import_rows)
                    import_rows = []
                total_count += 1
            index += 1
        except Exception as err:
            tasks_status_map[task_id]['err_msg'] = 'Row updated error'
            tasks_status_map[task_id]['status'] = 'terminated'
            tasks_status_map[task_id]['err_code'] = ROW_INSERT_ERROR_CODE
            dtable_io_logger.error(str(err))
            os.remove(file_path)
            return

    tasks_status_map[task_id]['status'] = status
    tasks_status_map[task_id]['rows_handled'] = total_count
    os.remove(file_path)
    return
