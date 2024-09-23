import io
import logging
import os
from queue import Queue, Full
from threading import Thread
from urllib.parse import urlparse, parse_qs

import requests

from seaserv import seafile_api

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_DB_URL
from dtable_events.convert_page.utils import get_chrome_data_dir, get_driver, open_page_view, wait_page_view
from dtable_events.dtable_io import batch_send_email_msg
from dtable_events.utils import get_inner_dtable_server_url, get_opt_from_conf_or_env
from dtable_events.utils.dtable_server_api import DTableServerAPI, NotFoundException
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()


class ConvertPageTOPDFManager:

    def __init__(self):
        self.max_workers = 2
        self.max_queue = 1000
        self.drivers = {}

    def init(self, config):
        section_name = 'CONERT-PAGE-TO-PDF'
        key_max_workers = 'max_workers'
        key_max_queue = 'max_queue'

        self.config = config

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
            driver = get_driver(get_chrome_data_dir(f'convert-manager-{index}'))
            self.drivers[index] = driver
        return driver

    def do_convert(self, index):
        while True:
            try:
                task_info = self.queue.get()
                logger.debug('do_convert task_info: %s', task_info)
                if task_info.get('action_type') == 'convert_page_to_pdf':
                    ConvertPageToPDFWorker(task_info, index, self).work()
                elif task_info.get('action_type') == 'convert_page_to_pdf_and_send':
                    ConvertPageToPDFAndSendWorker(task_info, index, self).work()
            except Exception as e:
                logger.exception('do task: %s error: %s', task_info, e)

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

    def clear_chrome(self, index):
        driver = self.drivers.get(index)
        if not driver:
            logger.debug('no index %s chrome', index)
            return
        try:  # delete all tab window except first blank
            logger.debug('i: %s driver.window_handles[1:]: %s', index, driver.window_handles[1:])
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
            self.manager.drivers.pop(index, None)


class ConvertPageToPDFWorker:

    def __init__(self, task_info, index, manager: ConvertPageTOPDFManager):
        self.task_info = task_info
        self.index = index
        self.manager = manager

    def batch_convert_rows(self, driver, repo_id, workspace_id, dtable_uuid, plugin_type, page_id, table_name, target_column, step_row_ids, file_names_dict):
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url, DTABLE_WEB_SERVICE_URL, repo_id, workspace_id)
        dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)
        rows_files_dict = {}
        row_session_dict = {}

        # open rows
        for row_id in step_row_ids:
            session_id = open_page_view(driver, dtable_uuid, plugin_type, page_id, row_id, dtable_server_api.internal_access_token)
            row_session_dict[row_id] = session_id

        # wait for chrome windows rendering
        for row_id in step_row_ids:
            output = io.BytesIO()  # receive pdf content
            session_id = row_session_dict[row_id]
            wait_page_view(driver, session_id, plugin_type, row_id, output)
            file_name = file_names_dict.get(row_id, f'{dtable_uuid}_{page_id}_{row_id}.pdf')
            if not file_name.endswith('.pdf'):
                file_name += '.pdf'
            file_info = dtable_server_api.upload_bytes_file(file_name, output.getvalue())
            rows_files_dict[row_id] = file_info

        # query rows
        row_ids_str = ', '.join(map(lambda row_id: f"'{row_id}'", step_row_ids))
        sql = f"SELECT `_id`, `{target_column['name']}` FROM `{table_name}` WHERE _id IN ({row_ids_str}) LIMIT {len(step_row_ids)}"
        try:
            rows, _ = dtable_db_api.query(sql)
        except Exception as e:
            logger.error('dtable: %s table: %s sql: %s error: %s', dtable_uuid, table_name, sql, e)
            return

        # update rows
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

    def check_resources(self, dtable_uuid, plugin_type, page_id, table_id, target_column_key, row_ids):
        """
        :return: resources -> dict or None, error_msg -> str or None
        """
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)
        dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)

        # metdata with plugin
        try:
            metadata = dtable_server_api.get_metadata_plugin(plugin_type)
        except NotFoundException:
            return None, 'base not found'
        except Exception as e:
            logger.error('plugin: %s dtable: %s get metadata error: %s', plugin_type, dtable_uuid, e)
            return None, 'get metadata error %s' % e

        # table
        table = next(filter(lambda t: t['_id'] == table_id, metadata.get('tables', [])), None)
        if not table:
            return None, 'table not found'

        # plugin
        plugin_settings = metadata.get('plugin_settings') or {}
        plugin = plugin_settings.get(plugin_type) or []
        if not plugin:
            return None, 'plugin not found'
        page = next(filter(lambda page: page.get('page_id') == page_id, plugin), None)
        if not page:
            return None, 'page %s not found' % page_id

        # column
        target_column = next(filter(lambda col: col['key'] == target_column_key, table.get('columns', [])), None)
        if not target_column:
            return None, 'column %s not found' % target_column_key

        # rows
        row_ids_str = ', '.join(map(lambda row_id: f"'{row_id}'", row_ids))
        sql = f"SELECT _id FROM `{table['name']}` WHERE _id IN ({row_ids_str}) LIMIT {len(row_ids)}"
        try:
            rows, _ = dtable_db_api.query(sql)
        except Exception as e:
            logger.error('plugin: %s dtable: %s query rows error: %s', plugin_type, dtable_uuid, e)
            return None, 'query rows error'
        row_ids = [row['_id'] for row in rows]

        return {
            'table': table,
            'target_column': target_column,
            'page': page,
            'row_ids': row_ids
        }, None

    def work(self):
        dtable_uuid = self.task_info.get('dtable_uuid')
        plugin_type = self.task_info.get('plugin_type')
        page_id = self.task_info.get('page_id')
        row_ids = self.task_info.get('row_ids')
        target_column_key = self.task_info.get('target_column_key')
        repo_id = self.task_info.get('repo_id')
        workspace_id = self.task_info.get('workspace_id')
        file_names_dict = self.task_info.get('file_names_dict')
        table_id = self.task_info.get('table_id')

        if not row_ids:
            return

        # resource check
        # Rather than wait one minute to render a wrong page, a resources check is more effective
        try:
            resources, error_msg = self.check_resources(dtable_uuid, plugin_type, page_id, table_id, target_column_key, row_ids)
            if not resources:
                logger.warning('plugin: %s dtable: %s page: %s task_info: %s error: %s', plugin_type, dtable_uuid, page_id, self.task_info, error_msg)
                return
            row_ids = resources['row_ids']
            table = resources['table']
            target_column = resources['target_column']
        except Exception as e:
            logger.exception('plugin: %s dtable: %s page: %s task_info: %s resource check error: %s', plugin_type, dtable_uuid, page_id, self.task_info, e)
            return

        try:
            # open all tabs of rows step by step
            # wait render and convert to pdf one by one
            step = 10
            for i in range(0, len(row_ids), step):
                step_row_ids = row_ids[i: i+step]
                try:
                    driver = self.manager.get_driver(self.index)
                except Exception as e:
                    logger.exception('get driver: %s error: %s', self.index, e)
                    continue
                try:
                    self.batch_convert_rows(driver, repo_id, workspace_id, dtable_uuid, plugin_type, page_id, table['name'], target_column, step_row_ids, file_names_dict)
                except Exception as e:
                    logger.exception('convert task: %s error: %s', self.task_info, e)
                finally:
                    self.manager.clear_chrome(self.index)
        except Exception as e:
            logger.exception(e)


class ConvertPageToPDFAndSendWorker:

    WECHAT_FILE_LIMIT = 20 << 20

    def __init__(self, task_info, index, manager: ConvertPageTOPDFManager):
        self.task_info = task_info
        self.index = index
        self.manager = manager

    def check_resources(self, dtable_uuid, plugin_type, page_id):
        """
        :return: resources -> dict or None, error_msg -> str or None
        """
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, get_inner_dtable_server_url())
        try:
            metadata = dtable_server_api.get_metadata_plugin(plugin_type)
        except NotFoundException:
            return None, 'base not found'
        except Exception as e:
            logger.error('plugin: %s dtable: %s get metadata error: %s', plugin_type, dtable_uuid, e)
            return None, 'get metadata error %s' % e

        # plugin
        plugin_settings = metadata.get('plugin_settings') or {}
        plugin = plugin_settings.get(plugin_type) or []
        if not plugin:
            return None, 'plugin not found'
        page = next(filter(lambda page: page.get('page_id') == page_id, plugin), None)
        if not page:
            return None, 'page %s not found' % page_id

        return {
            'page': page
        }, None

    def send_wechat_robot(self, file_name, output):
        if output.tell() >= self.WECHAT_FILE_LIMIT:
            logger.warning('convert task: %s generate pdf size exceeds', self.task_info)
            return

        account_info = self.task_info.get('account_info')
        detail = account_info.get('detail') or {}
        webhook_url = detail.get('webhook_url')
        if not webhook_url:
            return
        parsed_url = urlparse(webhook_url)
        query_params = parse_qs(parsed_url.query)
        key = query_params.get('key')[0]
        upload_url = f'{parsed_url.scheme}://{parsed_url.netloc}/cgi-bin/webhook/upload_media?key={key}&type=file'
        output.seek(0)
        resp = requests.post(upload_url, files={'file': (file_name, output)})
        if not resp.ok:
            logger.error('task: %s upload file error status code: %s', self.task_info, resp.status_code)
            return
        media_id = resp.json().get('media_id')
        msg_resp = requests.post(webhook_url, json={
            'msgtype': 'file',
            'file': {
                'media_id': media_id
            }
        })
        if not msg_resp.ok:
            logger.error('task: %s send file error status code: %s', self.task_info, resp.status_code)

    def send_email(self, file_name, output):
        account_info = self.task_info.get('account_info')
        detail = account_info.get('detail') or {}

        subject = self.task_info.get('subject')
        message = self.task_info.get('message')
        send_to_list = self.task_info.get('send_to_list')
        copy_to_list = self.task_info.get('copy_to_list')
        reply_to = self.task_info.get('reply_to')

        if not send_to_list:
            return

        send_info = {
            'subject': subject,
            'message': message,
            'send_to': send_to_list,
            'copy_to': copy_to_list,
            'reply_to': reply_to if reply_to else '',
            'file_contents': {file_name: output.getvalue()}
        }
        try:
            batch_send_email_msg(
                detail,
                [send_info],
                'automation-rules',
                config=self.manager.config
            )
        except Exception as e:
            logger.exception('task: %s send file email error: %s', self.task_info, e)

    def work(self):
        dtable_uuid = self.task_info.get('dtable_uuid')
        page_id = self.task_info.get('page_id')
        plugin_type = self.task_info.get('plugin_type')
        send_type = self.task_info.get('send_type')
        file_name = self.task_info.get('file_name')

        # check resources
        try:
            resources, error = self.check_resources(dtable_uuid, plugin_type, page_id)
            if not resources:
                logger.warning('plugin: %s dtable: %s page: %s task_info: %s error: %s', plugin_type, dtable_uuid, page_id, self.task_info, error)
                return
        except Exception as e:
            logger.exception('plugin: %s dtable: %s page: %s task_info: %s check resources error: %s', plugin_type, dtable_uuid, page_id, self.task_info, error)
            return

        # do convert
        try:
            driver = self.manager.get_driver(self.index)
        except Exception as e:
            logger.exception('task: %s get driver error: %s', self.task_info, e)
            return
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, get_inner_dtable_server_url())
        output = io.BytesIO()  # receive pdf content
        try:
            session_id = open_page_view(driver, dtable_uuid, plugin_type, page_id, None, dtable_server_api.internal_access_token)
            wait_page_view(driver, session_id, plugin_type, None, output)
        except Exception as e:
            logger.exception('convert task: %s error: %s', self.task_info, e)
            return
        finally:
            self.manager.clear_chrome(self.index)

        # send
        if not file_name:
            file_name = resources['page'].get('page_name') or 'document'
        file_name = str(file_name).strip()
        if not file_name.endswith('.pdf'):
            file_name = f'{file_name}.pdf'
        try:
            if send_type == 'email':
                self.send_email(file_name, output)
            elif send_type == 'wechat_robot':
                self.send_wechat_robot(file_name, output)
        except Exception as e:
            logger.exception('handle task: %s error: %s', self.task_info, e)


conver_page_to_pdf_manager = ConvertPageTOPDFManager()
