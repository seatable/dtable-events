import jwt
import pandas as pd
import logging
import os
import requests
import datetime
import json

# DTABLE_WEB_DIR
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
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
    FILE_SERVER_ROOT = getattr(seahub_settings, 'FILE_SERVER_ROOT', 'http://127.0.0.1:8082')
    SEATABLE_FAAS_AUTH_TOKEN = getattr(seahub_settings, 'SEATABLE_FAAS_AUTH_TOKEN')
    SEATABLE_FAAS_URL = getattr(seahub_settings, 'SEATABLE_FAAS_URL')
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

    def insert_row(self, rows):
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


def record_start_point(db_session, task_id, dtable_uuid, status, type):
    sql = '''
        INSERT INTO big_data_task_log (task_id, dtable_uuid, started_at, status, type) VALUES 
        (:task_id, :dtable_uuid, :started_at, :status, :type)
    '''
    db_session.execute(sql, {
        'task_id': task_id,
        'dtable_uuid': dtable_uuid,
        'started_at': datetime.datetime.now(),
        'status': status,
        'type': type,
    })
    db_session.commit()

def record_running_point(db_session, task_id, status):
    sql = '''
          UPDATE big_data_task_log SET status=:status WHERE task_id=:task_id;
        '''

    db_session.execute(sql, {
        'task_id': task_id,
        'status': status,
    })
    db_session.commit()

def record_end_point(db_session, task_id, status, detail):
    sql = '''
      UPDATE big_data_task_log SET finished_at=:finished_at, status=:status, detail=:detail WHERE task_id=:task_id;
    '''

    db_session.execute(sql, {
        'task_id': task_id,
        'finished_at': datetime.datetime.now(),
        'status': status,
        'detail': json.dumps(detail)
    })
    db_session.commit()


def match_columns(authed_base, table_name, target_columns):
    table_columns = authed_base.list_columns(table_name)
    for col in table_columns:
        col_type = col.get('type')
        if col_type in AUTO_GENERATED_COLUMNS:
            continue
        col_name = col.get('name')
        if col_name not in target_columns:
            return False, col_name

    return True, None



def handle_status_and_tmp_file(status_map, task_id, file_path):
    if status_map.get(task_id):
        status_map[task_id] = 'done'
    os.remove(file_path)

def import_excel_to_db(
        username,
        dtable_uuid,
        table_name,
        file_name,
        start_row,
        request_entity,
        file_path,
        db_session,
        task_id,
        tasks_status_map,

):
    from seatable_api import Base
    import time
    import numpy as np
    task_type = 'big_excel_import_task'
    try:
        entity = int(request_entity)
        start_row = int(start_row)
    except:
        entity = 500
        start_row = 0



    detail = {
        'err_msg': None,
        'start_row_num': start_row,
        'end_row_num': 0,
        'entity_size':entity,
        'file_name': file_name,
    }
    record_start_point(db_session, task_id, dtable_uuid, 'initializing', 'excel-import')
    df = pd.read_excel(file_path)
    df.replace(np.nan, '', regex=True, inplace=True)
    if start_row:
        df = df.iloc[int(start_row):, :]
    try:
        api_token = jwt.encode({
            'username': username,
            'dtable_uuid': dtable_uuid,
            'exp': time.time() + 60 * 10
        }, DTABLE_PRIVATE_KEY, algorithm='HS256')

        excel_columns = df.columns.tolist()
        base = Base(api_token, DTABLE_WEB_SERVICE_URL)
        base.auth()
        column_matched, column_name = match_columns(base, table_name, excel_columns)
        if not column_matched:
            detail['err_msg'] = 'Column %s does not match in excel' % column_name
            status = 'terminated'
            record_end_point(db_session, task_id, status, detail)
            handle_status_and_tmp_file(tasks_status_map, task_id, file_path)
            return

        db_handler = DBHandler(base, table_name)
    except Exception as err:
        detail['err_msg'] = str(err)
        status = 'terminated'
        record_end_point(db_session, task_id, status, detail)
        handle_status_and_tmp_file(tasks_status_map, task_id, file_path)
        return

    total_count = 0
    insert_count = 0
    slice = []
    total_rows = df.shape[0]
    if total_rows > 100000:
        detail['err_msg'] = 'Number of rows (%s) exceeds 100,000 limit' % total_rows
        status = 'terminated'
        record_end_point(db_session, task_id, status, detail)
        handle_status_and_tmp_file(tasks_status_map, task_id, file_path)
        return

    status = 'success'
    record_running_point(db_session, task_id, 'running')
    for index, d in df.iterrows():
        try:
            slice.append(d.to_dict())
            if total_count + 1 == total_rows or len(slice) == entity:
                if tasks_status_map.get(task_id) == 'cancelled':
                    status = 'cancelled'
                    break
                resp_content, err = db_handler.insert_row(slice)
                if err:
                    status = 'terminated'
                    detail['err_msg'] = str(resp_content)
                    break
                insert_count += len(slice)
                slice = []
                time.sleep(0.5)
            total_count += 1
        except Exception as err:
            detail['err_msg'] = str(err)
            status = 'terminated'
            break

    detail['end_row_num'] = insert_count + int(start_row)
    record_end_point(db_session, task_id, status, detail)
    handle_status_and_tmp_file(tasks_status_map, task_id, file_path)
    return
