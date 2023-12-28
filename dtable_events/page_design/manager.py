import io
import logging
import os
from queue import Queue, Full
from threading import Thread

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_DB_URL
from dtable_events.page_design.utils import get_driver, CHROME_DATA_DIR, open_page_view, wait_page_view
from dtable_events.utils import get_inner_dtable_server_url, get_opt_from_conf_or_env
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()

class ConvertPageTOPDFManager:

    def __init__(self):
        self.max_workers = 2
        self.max_queue = 200
        self.drivers = {}

    def init(self, config):
        section_name = 'CONERT-PAGE-TO-PDF'
        key_max_workers = 'max_workers'
        key_max_queue = 'max_queue'

        if config.has_section('CONERT-PAGE-TO-PDF'):
            try:
                self.max_workers = int(get_opt_from_conf_or_env(config, section_name, key_max_workers, default=self.max_workers))
            except:
                pass
            try:
                self.max_queue = int(get_opt_from_conf_or_env(config, section_name, key_max_queue, default=self.max_queue))
            except:
                pass
        self.queue = Queue(self.max_queue)  # element in queue is a dict about task
        try:  # kill all existing chrome processes
            os.system("ps aux | grep chrome | grep -v grep | awk ' { print $2 } ' | xargs kill -9 > /dev/null 2>&1")
        except:
            pass

    def get_driver(self, index):
        driver = self.drivers.get(index)
        if not driver:
            driver = get_driver(os.path.join(CHROME_DATA_DIR, f'convert-manager-{index}'))
            self.drivers[index] = driver
        return driver

    def batch_convert_rows(self, driver, repo_id, workspace_id, dtable_uuid, page_id, table_name, target_column, step_row_ids, file_names_dict):
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url, DTABLE_WEB_SERVICE_URL, repo_id, workspace_id)
        dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)
        rows_files_dict = {}
        row_session_dict = {}
        for row_id in step_row_ids:
            session_id = open_page_view(driver, dtable_uuid, page_id, row_id, dtable_server_api.internal_access_token)
            row_session_dict[row_id] = session_id
        for row_id in step_row_ids:
            output = io.BytesIO()  # receive pdf content
            session_id = row_session_dict[row_id]
            wait_page_view(driver, session_id, row_id, output)
            file_name = file_names_dict.get(row_id, f'{dtable_uuid}_{page_id}_{row_id}.pdf')
            if not file_name.endswith('.pdf'):
                file_name += '.pdf'
            file_info = dtable_server_api.upload_bytes_file(file_name, output.getvalue())
            rows_files_dict[row_id] = file_info
        row_ids_str = ', '.join(map(lambda row_id: f"'{row_id}'", step_row_ids))
        sql = f"SELECT `_id`, `{target_column['name']}` FROM `{table_name}` WHERE _id IN ({row_ids_str})"
        try:
            rows, _ = dtable_db_api.query(sql)
        except Exception as e:
            logger.error('dtable: %s table: %s sql: %s error: %s', dtable_uuid, table_name, sql, e)
            return
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

    def do_convert(self, index):
        while True:
            task_info = self.queue.get()
            logger.debug('do_convert task_info: %s', task_info)

            dtable_uuid = task_info.get('dtable_uuid')
            page_id = task_info.get('page_id')
            row_ids = task_info.get('row_ids')
            target_column = task_info.get('target_column')
            repo_id = task_info.get('repo_id')
            workspace_id = task_info.get('workspace_id')
            file_names_dict = task_info.get('file_names_dict')
            table_name = task_info.get('table_name')

            try:
                # open all tabs of rows step by step
                # wait render and export to pdf one by one
                step = 10
                for i in range(0, len(row_ids), step):
                    step_row_ids = row_ids[i: i+step]
                    try:
                        driver = self.get_driver(index)
                    except Exception as e:
                        logger.exception('get driver: %s error: %s', index, e)
                    try:
                        self.batch_convert_rows(driver, repo_id, workspace_id, dtable_uuid, page_id, table_name, target_column, step_row_ids, file_names_dict)
                    except Exception as e:
                        logger.exception('convert task: %s error: %s', task_info, e)
                    finally:
                        try:  # delete all tab window except first blank
                            logger.debug('i: %s driver.window_handles[1:]: %s', i, driver.window_handles[1:])
                            for window in driver.window_handles[1:]:
                                driver.switch_to.window(window)
                                driver.close()
                            # switch to the first tab window or error will occur when open new window
                            driver.switch_to.window(driver.window_handles[0])
                        except Exception as e:
                            logger.exception('close driver: %s error: %s', index, e)
                            try:
                                driver.quit()
                            except Exception as e:
                                logger.exception('quit driver: %s error: %s', index, e)
                            self.drivers.pop(index, None)
            except Exception as e:
                logger.exception(e)

    def start(self):
        logger.debug('convert page to pdf max workers: %s max queue: %s', self.max_workers, self.max_queue)
        for i in range(self.max_workers):
            t_name = f'driver-{i}'
            t = Thread(target=self.do_convert, args=(i,), name=t_name, daemon=True)
            t.start()

    def add_task(self, task_info):
        try:
            logger.debug('add task_info: %s', task_info)
            self.queue.put(task_info, block=False)
        except Full as e:
            logger.warning('convert queue full task: %s will be ignored', task_info)
            raise e


conver_page_to_pdf_manager = ConvertPageTOPDFManager()
