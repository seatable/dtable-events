import io
import logging
import os
from queue import Queue, Full
from threading import Thread

from sqlalchemy import text

from dtable_events.app.config import INNER_DTABLE_DB_URL, INNER_DTABLE_SERVER_URL
from dtable_events.convert_page.utils import get_chrome_data_dir, get_driver, open_page_view, wait_page_view, \
    get_documents_config, gen_page_design_pdf_view_url, gen_document_pdf_view_url
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, uuid_str_to_32_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI, NotFoundException
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)


class ConvertPageTOPDFManager:

    def __init__(self):
        self.max_workers = 2
        self.max_queue = 1000
        self.drivers = {}

    def init(self, config):
        section_name = 'CONVERT PAGE TO PDF'
        key_max_workers = 'max_workers'
        key_max_queue = 'max_queue'

        self.config = config
        self.session_class = init_db_session_class(self.config)

        if config.has_section(section_name):
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

    def convert_page_design_pdf(self, driver, resources):
        dtable_uuid = self.task_info.get('dtable_uuid')
        page_id = self.task_info.get('page_id')
        action_type = self.task_info.get('action_type')
        per_converted_callbacks = self.task_info.get('per_converted_callbacks') or []
        all_converted_callbacks = self.task_info.get('all_converted_callbacks') or []

        row_ids = resources.get('row_ids')
        # resources in convert-page-to-pdf action
        table = resources.get('table')
        target_column = resources.get('target_column')

        monitor_dom_id = 'page-design-render-complete'

        db_session = self.manager.session_class()
        sql = "SELECT `owner`, `org_id` FROM dtables d JOIN workspaces w ON d.workspace_id=w.id WHERE d.uuid=:dtable_uuid"
        try:
            result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
        except Exception as e:
            logger.error(f'query dtable {dtable_uuid} owner, org_id error {e}')
            return
        finally:
            db_session.close()
        kwargs = {'org_id': result.org_id, 'owner_id': result.owner}
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL, kwargs=kwargs)

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
                    url = gen_page_design_pdf_view_url(dtable_uuid, page_id, dtable_server_api.internal_access_token, row_id)
                    logger.debug('check page design url: %s', url)
                    session_id = open_page_view(driver, url)
                    row_session_dict[row_id] = session_id

                # wait for chrome windows rendering
                for row_id in step_row_ids:
                    output = io.BytesIO()  # receive pdf content
                    session_id = row_session_dict[row_id]
                    wait_page_view(driver, session_id, monitor_dom_id, row_id, output)
                    # per converted callbacks
                    pdf_content = output.getvalue()
                    if action_type == 'convert_page_to_pdf':
                        for callback in per_converted_callbacks:
                            try:
                                callback(row_id, pdf_content)
                            except Exception as e:
                                logger.exception(e)
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
                    logger.exception(e)

    def convert_document_pdf(self, driver):
        dtable_uuid = self.task_info.get('dtable_uuid')
        doc_uuid = self.task_info.get('doc_uuid')
        action_type = self.task_info.get('action_type')
        per_converted_callbacks = self.task_info.get('per_converted_callbacks') or []

        db_session = self.manager.session_class()
        sql = "SELECT `owner`, `org_id` FROM dtables d JOIN workspaces w ON d.workspace_id=w.id WHERE d.uuid=:dtable_uuid"
        try:
            result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
        except Exception as e:
            logger.error(f'query dtable {dtable_uuid} owner, org_id error {e}')
            return
        finally:
            db_session.close()
        kwargs = {'org_id': result.org_id, 'owner_id': result.owner}
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL, kwargs=kwargs)
        monitor_dom_id = 'document-render-complete'

        output = io.BytesIO()  # receive pdf content

        row_id = None
        url = gen_document_pdf_view_url(dtable_uuid, doc_uuid, dtable_server_api.internal_access_token, row_id)
        logger.debug('check document url: %s', url)

        session_id = open_page_view(driver, url)
        wait_page_view(driver, session_id, monitor_dom_id, None, output)
        # per converted callback
        pdf_content = output.getvalue()
        if action_type == 'convert_document_to_pdf_and_send':
            for callback in per_converted_callbacks:
                try:
                    callback(pdf_content)
                except Exception as e:
                    logger.exception(e)

    def check_document_resources(self, repo_id, dtable_uuid, doc_uuid):
        """
        :return: resources -> dict or None, error_msg -> str or None
        """
        document = get_documents_config(repo_id, dtable_uuid, 'dtable-events')
        if not document:
            return None, 'document not found'
        doc = next(filter(lambda doc: doc.get('doc_uuid') == doc_uuid, document), None)
        if not doc:
            return None, 'doc %s not found' % doc_uuid

        return {
                   'doc': doc,
               }, None

    def check_page_design_resources(self, dtable_uuid, page_id, table_id, target_column_key, row_ids):
        """
        :return: resources -> dict or None, error_msg -> str or None
        """
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL)
        dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)

        plugin_type = 'page-design'

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
        # when convert page-design to pdf page_id is not None
        page_id = self.task_info.get('page_id')
        # when convert document to pdf doc_uuid is not None
        doc_uuid = self.task_info.get('doc_uuid')
        table_id = self.task_info.get('table_id')
        target_column_key = self.task_info.get('target_column_key')
        row_ids = self.task_info.get('row_ids')
        repo_id = self.task_info.get('repo_id')

        if plugin_type == 'page-design':
            try:
                resources, error_msg = self.check_page_design_resources(dtable_uuid, page_id, table_id, target_column_key, row_ids)
                if not resources:
                    logger.warning('plugin: %s dtable: %s page: %s task_info: %s error: %s', plugin_type, dtable_uuid, page_id, self.task_info, error_msg)
                    return
            except Exception as e:
                logger.exception('plugin: %s dtable: %s page: %s task_info: %s resource check error: %s', plugin_type, dtable_uuid, page_id, self.task_info, e)
                return

            try:
                driver = self.manager.get_driver(self.index)
            except Exception as e:
                logger.exception('get driver: %s error: %s', self.index, e)
                return

            try:
                self.convert_page_design_pdf(driver, resources)
            except Exception as e:
                logger.exception(e)
            finally:
                self.manager.clear_chrome(self.index)

        elif plugin_type == 'document':
            try:
                resources, error_msg = self.check_document_resources(repo_id, dtable_uuid, doc_uuid)
                if not resources:
                    logger.warning('plugin: %s dtable: %s document: %s task_info: %s error: %s', plugin_type, dtable_uuid,
                                   doc_uuid, self.task_info, error_msg)
                    return
            except Exception as e:
                logger.exception('plugin: %s dtable: %s document: %s task_info: %s resource check error: %s', plugin_type,
                                 dtable_uuid, doc_uuid, self.task_info, e)
                return

            try:
                driver = self.manager.get_driver(self.index)
            except Exception as e:
                logger.exception('get driver: %s error: %s', self.index, e)
                return

            try:
                self.convert_document_pdf(driver)
            except Exception as e:
                logger.exception(e)
            finally:
                self.manager.clear_chrome(self.index)


conver_page_to_pdf_manager = ConvertPageTOPDFManager()
