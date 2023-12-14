import io
import logging
import os
from queue import Queue, Full
from threading import Thread

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_DB_URL
from dtable_events.page_design.utils import convert_page_to_pdf, get_driver, CHROME_DATA_DIR
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()

class ConvertPageTOPDFManager:

    def __init__(self):
        self.max_workers = 10

    def init(self, config):
        self.config = config
        self.max_workers = 10
        self.queue = Queue(self.max_workers)  # element in queue is a dict about task
        try:  # kill all existing chrome processes
            os.system("ps aux | grep chrome | grep -v grep | awk ' { print $2 } ' | xargs kill -9")
        except:
            pass
        self.drivers = [get_driver(os.path.join(CHROME_DATA_DIR, f'convert-manager-{i}')) for i in range(self.max_workers)]

    def do_convert(self, index):
        while True:
            task_info = self.queue.get()
            logger.debug('do_convert task_info: %s', task_info)
            try:
                dtable_uuid = task_info.get('dtable_uuid')
                page_id = task_info.get('page_id')
                row_ids = task_info.get('row_ids')
                target_column = task_info.get('target_column')
                repo_id = task_info.get('repo_id')
                workspace_id = task_info.get('workspace_id')
                file_names_dict = task_info.get('file_names_dict')
                table_name = task_info.get('table_name')
                dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url, DTABLE_WEB_SERVICE_URL, repo_id, workspace_id)
                dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)
                rows_files_dict = {}
                for row_id in row_ids:
                    output = io.BytesIO()
                    try:
                        convert_page_to_pdf(self.drivers[index], dtable_uuid, page_id, row_id, dtable_server_api.access_token, output)
                        file_name = file_names_dict.get(row_id, f'{dtable_uuid}_{page_id}_{row_id}.pdf')
                        if not file_name.endswith('.pdf'):
                            file_name += '.pdf'
                        file_info = dtable_server_api.upload_bytes_file(file_name, output.getvalue())
                        rows_files_dict[row_id] = file_info
                    except Exception as e:
                        logger.exception('convert dtable: %s page: %s row: %s error: %s', dtable_uuid, page_id, row_id, e)
                        continue
                row_ids_str = ', '.join(map(lambda row_id: f"'{row_id}'", row_ids))
                sql = f"SELECT `_id`, `{target_column['name']}` FROM `{table_name}` WHERE _id IN ({row_ids_str})"
                try:
                    rows, _ = dtable_db_api.query(sql)
                except Exception as e:
                    logger.error('dtable: %s table: %s sql: %s error: %s', dtable_uuid, table_name, sql, e)
                    continue
                updates = []
                for row in rows:
                    row_id = row['_id']
                    files = row.get(target_column['name']) or []
                    files.append(rows_files_dict[row_id])
                    updates.append({
                        'row_id': row_id,
                        'row': {target_column['name']: files}
                    })
                dtable_server_api.batch_update_rows(table_name, updates)
            except Exception as e:
                logger.exception('convert task: %s error: %s', task_info, e)

    def start(self):
        for i in range(self.max_workers):
            t_name = f'driver-{i}'
            t = Thread(target=self.do_convert, args=(i,), name=t_name, daemon=True)
            t.start()

    def add_task(self, task_info):
        try:
            logger.debug('add task_info: %s', task_info)
            self.queue.put(task_info, block=False)
        except Full:
            logger.warning('convert queue full task: %s will be ignored', task_info)


conver_page_to_pdf_manager = ConvertPageTOPDFManager()
