import jwt
import openpyxl
import logging
import os
import requests
import datetime
import json

# DTABLE_WEB_DIR
from dtable_events.dtable_io.excel import parse_row
from dtable_events.utils.constants import ColumnTypes

dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

logger = logging.getLogger(__name__)


try:
    import seahub.settings as seahub_settings
    DTABLE_WEB_SERVICE_URL = getattr(seahub_settings, 'DTABLE_WEB_SERVICE_URL')
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)



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

class DBHandler(object):

    def __init__(self, authed_base, table_name):
        self.base = authed_base
        self.table_name = table_name
        self._ini()

    def _ini(self):

        self.db_url = self.base.dtable_db_url
        self.headers = self.base.headers
        self.dtable_uuid = self.base.dtable_uuid

    def insert_rows(self, rows):
        api_url = "%s/api/v1/insert-rows/%s" % (
            self.db_url.rstrip('/'),
            self.dtable_uuid
        )

        params = {
            "table_name": self.table_name,
            "rows": rows
        }
        resp = requests.post(api_url, json=params, headers=self.headers)
        if not resp.status_code == 200:
            return resp.text, True
        return resp.json(), False

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
    from seatable_api import Base
    from dtable_events.dtable_io import dtable_io_logger
    import time

    tasks_status_map[task_id] = {
        'status': 'initializing',
        'err_msg': '',
        'rows_imported': 0,
        'total_rows': 0,
    }
    try:
        wb = openpyxl.load_workbook(file_path, read_only=True)
        sheets = wb.get_sheet_names()
        ws = wb[sheets[0]]
        total_rows = ws.max_row and ws.max_row - 1 or 0
        if total_rows > 100000:
            tasks_status_map[task_id]['err_msg'] = 'Number of rows (%s) exceeds 100,000 limit' % total_rows
            tasks_status_map[task_id]['status'] = 'terminated'
            os.remove(file_path)
            return
    except Exception as err:
        tasks_status_map[task_id]['err_msg'] = "file reading error: %s" % str(err)
        tasks_status_map[task_id]['status'] = 'terminated'
        os.remove(file_path)
        return

    try:
        api_token = jwt.encode({
            'username': username,
            'dtable_uuid': dtable_uuid,
            'exp': time.time() + 60 * 10
        }, DTABLE_PRIVATE_KEY, algorithm='HS256')

        excel_columns = [cell.value for cell in ws[1]]
        base = Base(api_token, DTABLE_WEB_SERVICE_URL)
        base.auth()
        column_matched, column_name, base_columns = match_columns(base, table_name, excel_columns)
        if not column_matched:
            tasks_status_map[task_id]['err_msg'] = 'Column %s does not match in excel' % column_name
            tasks_status_map[task_id]['status'] = 'terminated'
            os.remove(file_path)
            return

        db_handler = DBHandler(base, table_name)
    except Exception as err:
        tasks_status_map[task_id]['err_msg'] = str(err)
        tasks_status_map[task_id]['status'] = 'terminated'
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
        err_flag = False
        try:
            if index > 0:
                row_list = [r.value for r in row]
                row_data = dict(zip(excel_columns, row_list))
                parsed_row_data = {}
                for col_name, value in row_data.items():
                    col_type = column_name_type_map.get(col_name)
                    parsed_row_data[col_name] = parse_row(col_type, value)
                slice.append(parsed_row_data)
                if total_count + 1 == total_rows or len(slice) == 100:
                    tasks_status_map[task_id]['rows_imported'] = insert_count
                    resp_content, err = db_handler.insert_rows(slice)
                    if err:
                        err_flag = True
                        dtable_io_logger.error('row insterted error: %s' % (resp_content))
                    insert_count += len(slice)
                    slice = []
                total_count += 1
            index += 1
        except Exception as err:
            err_flag = True
            dtable_io_logger.error(str(err))

        if err_flag:
            continue


    tasks_status_map[task_id]['status'] = status
    tasks_status_map[task_id]['rows_imported'] = insert_count
    os.remove(file_path)
    return
