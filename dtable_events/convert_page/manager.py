import io
import logging
import os
from queue import Queue, Full
from threading import Thread

from dtable_events.app.config import INNER_DTABLE_DB_URL
from dtable_events.convert_page.utils import get_chrome_data_dir, get_driver, open_page_view, wait_page_view, get_documents_config
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
                ConvertPageToPDFWorker(task_info, index, self).work()
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
            self.drivers.pop(index, None)


class ConvertPageToPDFWorker:

    def __init__(self, task_info, index, manager: ConvertPageTOPDFManager):
        self.task_info = task_info
        self.index = index
        self.manager = manager

    def convert_with_rows(self, driver, resources):
        dtable_uuid = self.task_info.get('dtable_uuid')
        plugin_type = self.task_info.get('plugin_type')
        page_id = self.task_info.get('page_id')
        action_type = self.task_info.get('action_type')
        per_converted_callbacks = self.task_info.get('per_converted_callbacks') or []
        all_converted_callbacks = self.task_info.get('all_converted_callbacks') or []

        row_ids = resources.get('row_ids')
        # resources in convert-page-to-pdf action
        table = resources.get('table')
        target_column = resources.get('target_column')

        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)

        # convert
        # open all tabs of rows step by step
        # wait render and convert to pdf one by one
        step = 10
        for i in range(0, len(row_ids), step):
            try:
                step_row_ids = row_ids[i: i+step]
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
                    # per converted callbacks
                    pdf_content = output.getvalue()
                    if action_type == 'convert_page_to_pdf':
                        for callback in per_converted_callbacks:
                            try:
                                callback(row_id, pdf_content)
                            except Exception as e:
                                logging.exception(e)
            except Exception as e:
                logger.exception('convert task: %s error: %s', self.task_info, e)
                continue
            finally:
                self.manager.clear_chrome(self.index)

        # callbacks
        if action_type == 'convert_page_to_pdf':
            for callback in all_converted_callbacks:
                try:
                    callback(table, target_column)
                except Exception as e:
                    logging.exception(e)

    def convert_without_rows(self, driver):
        dtable_uuid = self.task_info.get('dtable_uuid')
        plugin_type = self.task_info.get('plugin_type')
        page_id = self.task_info.get('page_id')
        action_type = self.task_info.get('action_type')
        per_converted_callbacks = self.task_info.get('per_converted_callbacks') or []

        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)

        output = io.BytesIO()  # receive pdf content
        session_id = open_page_view(driver, dtable_uuid, plugin_type, page_id, None, dtable_server_api.internal_access_token)
        wait_page_view(driver, session_id, plugin_type, None, output)
        # per converted callback
        pdf_content = output.getvalue()
        if action_type == 'convert_document_to_pdf_and_send':
            for callback in per_converted_callbacks:
                try:
                    callback(pdf_content)
                except Exception as e:
                    logging.exception(e)

    def check_resources(self, repo_id, dtable_uuid, plugin_type, page_id, table_id, target_column_key, row_ids):
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
        if table_id:
            table = next(filter(lambda t: t['_id'] == table_id, metadata.get('tables', [])), None)
            if not table:
                return None, 'table not found'
        else:
            table = None

        # plugin
        plugin_settings = metadata.get('plugin_settings') or {}
        plugin = plugin_settings.get(plugin_type) or []
        if not plugin and plugin_type == 'document':
            plugin = get_documents_config(repo_id, dtable_uuid, 'dtable-events')

        if not plugin:
            return None, 'plugin not found'
        page = next(filter(lambda page: page.get('page_id') == page_id, plugin), None)
        if not page:
            return None, 'page %s not found' % page_id

        # column
        if target_column_key:
            target_column = next(filter(lambda col: col['key'] == target_column_key, table.get('columns', [])), None)
            if not target_column:
                return None, 'column %s not found' % target_column_key
        else:
            target_column = None

        # rows
        if row_ids:
            row_ids_str = ', '.join(map(lambda row_id: f"'{row_id}'", row_ids))
            sql = f"SELECT _id FROM `{table['name']}` WHERE _id IN ({row_ids_str}) LIMIT {len(row_ids)}"
            try:
                rows, _ = dtable_db_api.query(sql)
            except Exception as e:
                logger.error('plugin: %s dtable: %s query rows error: %s', plugin_type, dtable_uuid, e)
                return None, 'query rows error'
            row_ids = [row['_id'] for row in rows]
        else:
            row_ids = None

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
        table_id = self.task_info.get('table_id')
        target_column_key = self.task_info.get('target_column_key')
        row_ids = self.task_info.get('row_ids')
        repo_id = self.task_info.get('repo_id')

        # resource check
        # Rather than wait one minute to render a wrong page, a resources check is more effective
        try:
            resources, error_msg = self.check_resources(repo_id, dtable_uuid, plugin_type, page_id, table_id, target_column_key, row_ids)
            if not resources:
                logger.warning('plugin: %s dtable: %s page: %s task_info: %s error: %s', plugin_type, dtable_uuid, page_id, self.task_info, error_msg)
                return
            row_ids = resources.get('row_ids')
        except Exception as e:
            logger.exception('plugin: %s dtable: %s page: %s task_info: %s resource check error: %s', plugin_type, dtable_uuid, page_id, self.task_info, e)
            return

        try:
            driver = self.manager.get_driver(self.index)
        except Exception as e:
            logger.exception('get driver: %s error: %s', self.index, e)
            return

        try:
            if row_ids is not None:  # rows
                self.convert_with_rows(driver, resources)
            else:  # no rows
                self.convert_without_rows(driver)
        except Exception as e:
            logger.exception(e)
        finally:
            self.manager.clear_chrome(self.index)


conver_page_to_pdf_manager = ConvertPageTOPDFManager()
