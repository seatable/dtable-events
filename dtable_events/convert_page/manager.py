import asyncio
import io
import logging
import os
from queue import Queue, Full
from threading import Thread

from playwright.async_api import async_playwright
from playwright._impl._errors import TimeoutError
from sqlalchemy import text

from dtable_events.app.config import INNER_DTABLE_DB_URL, INNER_DTABLE_SERVER_URL, DTABLE_WEB_SERVICE_URL
from dtable_events.convert_page.process_monitor import browser_monitor
from dtable_events.convert_page.utils import get_documents_config, get_pdf_print_options, wait_for_images
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, uuid_str_to_32_chars, uuid_str_to_36_chars
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.dtable_server_api import DTableServerAPI, NotFoundException

logger = logging.getLogger(__name__)


class BrowserWorker(Thread):

    def __init__(self, index, task_queue: Queue, config, pages=10):
        super().__init__()
        self.thread_id = index
        self.task_queue = task_queue
        self.playwright = None
        self.browser = None
        self.context = None
        self.pages = pages

        self.db_session_class = init_db_session_class(config)

        self.is_browser_alive = False
        self.browser_pid = None

        self.loop = asyncio.new_event_loop()  # each thread has own event loop

    def disconnect_browser_cb(self):
        self.is_browser_alive = False
        self.browser = None
        self.context = None
        logger.error(f"Thread-{self.thread_id} browser disconnected... will use new browser")

    async def get_context(self):
        if not self.is_browser_alive:
            logger.info(f"Thread-{self.thread_id} browser make a new browser...")

        if self.context:
            return self.context

        if not self.playwright:
            self.playwright = await async_playwright().start()
        if not self.browser:
            self.is_browser_alive = True
            self.browser = await self.playwright.chromium.launch(executable_path='/usr/bin/google-chrome', headless=True)
            self.browser.on('disconnected', self.disconnect_browser_cb)
        self.context = await self.browser.new_context()
        self.browser_pid = self.browser._impl_obj._connection._transport._proc.pid
        browser_monitor.add_pid_info({self.browser_pid: {'name': f"BrowserWorker: Thread - {self.thread_id}"}})
        return self.context

    def check_resources(self, dtable_uuid, plugin_type, page_id, doc_uuid, table_id, target_column_key, row_ids, repo_id):
        """
        :return: resources -> dict or None, error_msg -> str or None
        """
        dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL)
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
        if not plugin:
            return None, 'plugin not found'
        page, doc = None, None
        if plugin_type == 'page-design':
            page = next(filter(lambda page: page.get('page_id') == page_id, plugin), None)
            if not page:
                return None, 'page %s not found' % page_id
        elif plugin_type == 'document':
            document = get_documents_config(repo_id, dtable_uuid, 'dtable-events')
            if not document:
                return None, 'document not found'
            doc = next(filter(lambda doc: doc.get('doc_uuid') == doc_uuid, document), None)
            if not doc:
                return None, 'document %s not found' % doc_uuid

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
            'doc':doc,
            'row_ids': row_ids
        }, None

    async def row_page_to_pdf(self, url, context, row_id, action_type, per_converted_callbacks):
        page = await context.new_page()
        page.on("request", lambda request: logger.debug(f"Request: {request.method} {request.url}"))
        page.on("response", lambda response: logger.debug(f"Response: {response.status} {response.url}"))
        page.on("console", lambda msg: logger.debug(f"Console [{msg.type}]: {msg.text}"))
        try:
            await page.goto(url, wait_until="load")
            await wait_for_images(page)
            content = await page.pdf(**get_pdf_print_options())
        except TimeoutError:
            content = await page.pdf(**get_pdf_print_options())
        await page.close()
        if action_type == 'convert_page_to_pdf':
            for callback in per_converted_callbacks:
                try:
                    callback(row_id, content)
                except Exception as e:
                    logger.exception(e)

    async def convert_with_rows(self, task_info, resources):
        dtable_uuid = task_info.get('dtable_uuid')
        plugin_type = task_info.get('plugin_type')
        page_id = task_info.get('page_id')
        action_type = task_info.get('action_type')
        per_converted_callbacks = task_info.get('per_converted_callbacks') or []
        all_converted_callbacks = task_info.get('all_converted_callbacks') or []

        row_ids = resources.get('row_ids')
        # resources in convert-page-to-pdf action
        table = resources.get('table')
        target_column = resources.get('target_column')

        # convert
        # open all tabs of rows pages by pages
        # wait render and convert to pdf one by one
        pages = self.pages

        # need org_id, owner
        db_session = self.db_session_class()
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
        for i in range(0, len(row_ids), pages):
            tasks = []
            context = await self.get_context()
            # open rows
            for row_id in row_ids[i: i+pages]:
                url = ''
                if plugin_type == 'page-design':
                    url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/row/%s/' % (uuid_str_to_36_chars(dtable_uuid), page_id, row_id)
                if not url:
                    continue
                url += '?access-token=%s&need_convert=%s' % (dtable_server_api.internal_access_token, 0)

                tasks.append(self.row_page_to_pdf(url, context, row_id, action_type, per_converted_callbacks))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    logger.exception(f'Thread-{self.thread_id} convert rows error: {e}')

        # callbacks
        if action_type == 'convert_page_to_pdf':
            for callback in all_converted_callbacks:
                try:
                    callback(table, target_column)
                except Exception as e:
                    logger.exception(e)

    async def convert_without_rows(self, task_info):
        dtable_uuid = task_info.get('dtable_uuid')
        plugin_type = task_info.get('plugin_type')
        doc_uuid = task_info.get('doc_uuid')
        action_type = task_info.get('action_type')
        per_converted_callbacks = task_info.get('per_converted_callbacks') or []

        url = ''
        if plugin_type == 'document':
            url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/document/%s/row/%s/' % (uuid_str_to_36_chars(dtable_uuid), doc_uuid, None)
        if not url:
            return

        # need org_id, owner
        db_session = self.db_session_class()
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
        url += '?access-token=%s&need_convert=%s' % (dtable_server_api.internal_access_token, 0)

        context = await self.get_context()
        page = await context.new_page()
        page.on("request", lambda request: logger.debug(f"Request: {request.method} {request.url}"))
        page.on("response", lambda response: logger.debug(f"Response: {response.status} {response.url}"))
        page.on("console", lambda msg: logger.debug(f"Console [{msg.type}]: {msg.text}"))
        try:
            await page.goto(url, wait_until="load")
            await page.wait_for_load_state('networkidle', timeout=180*1000)
            await page.wait_for_selector('#document-render-complete', timeout=60*1000)
            await wait_for_images(page)
            pdf_content = await page.pdf(**get_pdf_print_options())
        except TimeoutError:
            pdf_content = await page.pdf(**get_pdf_print_options())

        if action_type == 'convert_document_to_pdf_and_send':
            for callback in per_converted_callbacks:
                try:
                    callback(pdf_content)
                except Exception as e:
                    logger.exception(e)
        await page.close()

    async def _do_convert(self, task_info):
        dtable_uuid = task_info.get('dtable_uuid')
        plugin_type = task_info.get('plugin_type')
        page_id = task_info.get('page_id')
        doc_uuid = task_info.get('doc_uuid')
        table_id = task_info.get('table_id')
        target_column_key = task_info.get('target_column_key')
        row_ids = task_info.get('row_ids')
        repo_id = task_info.get('repo_id')

        # resource check
        # Rather than wait one minute to render a wrong page, a resources check is more effective
        try:
            resources, error_msg = self.check_resources(dtable_uuid, plugin_type, page_id, doc_uuid, table_id, target_column_key, row_ids, repo_id)
            if not resources:
                logger.warning('plugin: %s dtable: %s page: %s task_info: %s error: %s', plugin_type, dtable_uuid, page_id, task_info, error_msg)
                return
            row_ids = resources.get('row_ids')
        except Exception as e:
            logger.exception('plugin: %s dtable: %s page: %s task_info: %s resource check error: %s', plugin_type, dtable_uuid, page_id, task_info, e)
            return

        # browser context access url
        if row_ids:
            await self.convert_with_rows(task_info, resources)
        else:
            await self.convert_without_rows(task_info)

    async def do_convert(self, task_info):
        try:
            await self._do_convert(task_info)
        except Exception as e:
            logger.exception(f'do convert Thread-{self.thread_id} Exception in loop.run_until_complete - {e}')
            try:
                if self.browser:
                    await self.browser.close()
            except Exception as e:
                logger.exception(f'do convert Thread-{self.thread_id} close browser error: {e}')
            finally:
                self.context = None
                self.browser = None
                self.is_browser_alive = False
                if self.browser_pid:
                    browser_monitor.remove_pid(self.browser_pid)

    def run(self):
        asyncio.set_event_loop(self.loop)
        while True:
            task_info = self.task_queue.get()

            try:
                self.loop.run_until_complete(self.do_convert(task_info))
            except Exception as e:
                logger.exception(f'Thread-{self.thread_id} Exception in loop.run_until_complete - {e}')
            finally:
                browser_monitor.remove_pid(self.browser_pid)



class ConvertPageToPDFManager:

    def __init__(self):
        self.max_workers = 2
        self.max_queue = 1000
        self.pages = 10

    def init(self, config):
        section_name = 'CONVERT PAGE TO PDF'
        key_max_workers = 'max_workers'
        key_max_queue = 'max_queue'
        key_pages = 'pages'

        self.config = config

        if config.has_section('CONVERT PAGE TO PDF'):
            self.max_workers = int(get_opt_from_conf_or_env(config, section_name, key_max_workers, default=self.max_workers))
            self.max_queue = int(get_opt_from_conf_or_env(config, section_name, key_max_queue, default=self.max_queue))
            self.pages = int(get_opt_from_conf_or_env(config, section_name, key_pages, default=self.pages))

        self.queue = Queue(self.max_queue)  # element in queue is a dict about task

    def start(self):
        logger.debug('convert page to pdf max workers: %s max queue: %s pages: %s', self.max_workers, self.max_queue, self.pages)
        for i in range(self.max_workers):
            t = BrowserWorker(i, self.queue, self.config, self.pages)
            t.start()

    def add_task(self, task_info):
        try:
            logger.debug('add task_info: %s', task_info)
            self.queue.put(task_info, block=False)
        except Full as e:
            logger.warning('convert queue full task: %s will be ignored', task_info)
            raise e


conver_page_to_pdf_manager = ConvertPageToPDFManager()
