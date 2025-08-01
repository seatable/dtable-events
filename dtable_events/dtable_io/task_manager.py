import inspect
import os
import queue
import threading
import time
import uuid
from threading import Lock

from dtable_events.utils.utils_metric import publish_metric, TASK_MANAGER_METRIC_HELP

from seaserv import seafile_api


def log_function_call(func):

    def wrapper(*args, **kwargs):
        from dtable_events.dtable_io import dtable_io_logger

        func_name = func.__name__

        signature = inspect.signature(func)
        bound_args = signature.bind(*args, **kwargs)
        bound_args.apply_defaults()

        max_length_value = 100
        args_str_list = []
        for name, value in bound_args.arguments.items():
            if name == 'self':
                continue
            args_str_list.append(f"{name}: {str(value)[:max_length_value]+'...' if len(str(value)) > max_length_value else str(value)}")
        args_repr = ', '.join(args_str_list)

        dtable_io_logger.info("func: %s args: %s", func_name, args_repr)

        return func(*args, **kwargs)
    return wrapper


class TaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.task_results_map = {}
        self.tasks_queue = queue.Queue(10)
        self.config = None
        self.current_task_info = {}
        self.threads = []

        self.dataset_sync_ids = set()
        self.dataset_sync_ids_lock = Lock()

        self.force_sync_dataset_ids = set()
        self.force_sync_dataset_ids_lock = Lock()

        self.conf = {}

    def init(self, app, workers, file_server_port, io_task_timeout, config):
        self.app = app
        self.conf['file_server_port'] = file_server_port
        self.conf['io_task_timeout'] = io_task_timeout
        self.conf['workers'] = workers

        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in (self.tasks_map.keys() | self.task_results_map.keys())

    @log_function_call
    def add_export_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name, ignore_asset, is_export_folder, folder_path):
        from dtable_events.dtable_io import get_dtable_export_content, get_dtable_export_content_folder

        asset_dir_id = None
        if not ignore_asset:
            asset_dir_path = os.path.join('/asset', dtable_uuid)
            asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)

        task_id = str(uuid.uuid4())
        if not is_export_folder:
            task = (get_dtable_export_content,
                    (username, repo_id, workspace_id, dtable_uuid, asset_dir_id, self.config, task_id))
        else:
            task = (get_dtable_export_content_folder,
                    (username, repo_id, workspace_id, dtable_uuid, asset_dir_id, self.config, folder_path, task_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), metric_name='io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage,
                        can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id,
                        is_import_folder, folder_path):
        from dtable_events.dtable_io import post_dtable_import_files, post_dtable_import_files_folder

        task_id = str(uuid.uuid4())
        if not is_import_folder:
            task = (post_dtable_import_files,
                    (username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage,
                    can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id, self.config, task_id))
        else:
            task = (post_dtable_import_files_folder,
                    (username, repo_id, workspace_id, dtable_uuid, folder_path, in_storage,
                    can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id, self.config, task_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_export_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map=None):
        from dtable_events.dtable_io import get_dtable_export_asset_files

        task_id = str(uuid.uuid4())
        task = (get_dtable_export_asset_files,
                (username, repo_id, dtable_uuid, files, task_id, self.config, files_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_export_dtable_big_data_screen_task(self, username, repo_id, dtable_uuid, page_id):
        from  dtable_events.dtable_io import get_dtable_export_big_data_screen
        task_id = str(uuid.uuid4())
        task = (get_dtable_export_big_data_screen,
                (username, repo_id, dtable_uuid, page_id, task_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_dtable_big_data_screen_task(self, username, repo_id, dtable_uuid, page_id):
        from  dtable_events.dtable_io import import_big_data_screen
        task_id = str(uuid.uuid4())
        task = (import_big_data_screen,
                (username, repo_id, dtable_uuid, page_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_export_dtable_big_data_screen_app_task(self, username, repo_id, dtable_uuid, app_uuid, app_id):
        from  dtable_events.dtable_io import get_dtable_export_big_data_screen_app
        task_id = str(uuid.uuid4())
        task = (get_dtable_export_big_data_screen_app,
                (username, repo_id, dtable_uuid, app_uuid, app_id, task_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_dtable_big_data_screen_app_task(self, username, repo_id, dtable_uuid, app_uuid, app_id):
        from  dtable_events.dtable_io import import_big_data_screen_app
        task_id = str(uuid.uuid4())
        task = (import_big_data_screen_app,
                (username, repo_id, dtable_uuid, app_uuid, app_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_transfer_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map, parent_dir, relative_path, replace, repo_api_token, seafile_server_url):
        from dtable_events.dtable_io import get_dtable_transfer_asset_files
        task_id = str(uuid.uuid4())
        task = (get_dtable_transfer_asset_files,
                (username,
                 repo_id,
                 dtable_uuid,
                 files,
                 task_id,
                 files_map,
                 parent_dir,
                 relative_path,
                 replace,
                 repo_api_token,
                 seafile_server_url,
                 self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_parse_excel_csv_task(self, username, repo_id, file_name, file_type, parse_type, dtable_uuid):
        from dtable_events.dtable_io import parse_excel_csv

        task_id = str(uuid.uuid4())
        task = (parse_excel_csv,
                (username, repo_id, file_name, file_type, parse_type, dtable_uuid, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_excel_csv_task(self, username, repo_id, dtable_uuid, dtable_name, included_tables, lang):
        from dtable_events.dtable_io import import_excel_csv

        task_id = str(uuid.uuid4())
        task = (import_excel_csv,
                (username, repo_id, dtable_uuid, dtable_name, included_tables, lang, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_excel_csv_add_table_task(self, username, dtable_uuid, dtable_name, included_tables, lang):
        from dtable_events.dtable_io import import_excel_csv_add_table

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_add_table,
                (username, dtable_uuid, dtable_name, included_tables, lang, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_append_excel_csv_append_parsed_file_task(self, username, dtable_uuid, file_name, table_name):
        from dtable_events.dtable_io import append_excel_csv_append_parsed_file

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_append_parsed_file,
                (username, dtable_uuid, file_name, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_append_excel_csv_upload_file_task(self, username, file_name, dtable_uuid, table_name, file_type):
        from dtable_events.dtable_io import append_excel_csv_upload_file

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_upload_file,
                (username, file_name, dtable_uuid, table_name, file_type))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_run_auto_rule_task(self, automation_rule_id, username, org_id, dtable_uuid, run_condition, trigger, actions):
        from dtable_events.automations.auto_rules_utils import run_auto_rule_task
        task_id = str(uuid.uuid4())
        options = {
            'run_condition': run_condition,
            'dtable_uuid': dtable_uuid,
            'org_id': org_id,
            'creator': username,
            'rule_id': automation_rule_id
        }

        task = (run_auto_rule_task, (trigger, actions, options, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_update_excel_csv_update_parsed_file_task(self, username, dtable_uuid, file_name, table_name,
                                                     selected_columns, can_add_row, can_update_row):
        from dtable_events.dtable_io import update_excel_csv_update_parsed_file

        task_id = str(uuid.uuid4())
        task = (update_excel_csv_update_parsed_file,
                (username, dtable_uuid, file_name, table_name, selected_columns, can_add_row, can_update_row))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_update_excel_upload_excel_task(self, username, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_excel_upload_excel

        task_id = str(uuid.uuid4())
        task = (update_excel_upload_excel,
                (username, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_update_csv_upload_csv_task(self, username, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_csv_upload_csv

        task_id = str(uuid.uuid4())
        task = (update_csv_upload_csv,
                (username, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_excel_csv_to_dtable_task(self, username, repo_id, dtable_name, dtable_uuid, file_type, lang):
        from dtable_events.dtable_io import import_excel_csv_to_dtable

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_to_dtable, (username, repo_id, dtable_name, dtable_uuid, file_type, lang))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_import_excel_csv_to_table_task(self, username, file_name, dtable_uuid, file_type, lang):
        from dtable_events.dtable_io import import_excel_csv_to_table

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_to_table, (username, file_name, dtable_uuid, file_type, lang))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_update_table_via_excel_csv_task(self, username, file_name, dtable_uuid, table_name, selected_columns, file_type, can_add_row, can_update_row):
        from dtable_events.dtable_io import update_table_via_excel_csv

        task_id = str(uuid.uuid4())
        task = (update_table_via_excel_csv, (username, file_name, dtable_uuid, table_name, selected_columns, file_type, can_add_row, can_update_row))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    @log_function_call
    def add_append_excel_csv_to_table_task(self, username, file_name, dtable_uuid, table_name, file_type):
        from dtable_events.dtable_io import append_excel_csv_to_table

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_to_table, (username, file_name, dtable_uuid, table_name, file_type))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    def query_status(self, task_id):
        task_result = self.task_results_map.pop(task_id, None)
        if not task_result:
            return False, None
        return True, task_result

    def convert_page_design_to_pdf(self, dtable_uuid, page_id, row_id, username):
        from dtable_events.dtable_io import convert_page_design_to_pdf

        task_id = str(uuid.uuid4())
        task = (convert_page_design_to_pdf,
                (dtable_uuid, page_id, row_id, username, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    def convert_document_to_pdf(self, dtable_uuid, doc_uuid, row_id, username):
        from dtable_events.dtable_io import convert_document_to_pdf

        task_id = str(uuid.uuid4())
        task = (convert_document_to_pdf,
                (dtable_uuid, doc_uuid, row_id, username, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_import_table_from_base_task(self, context):
        from dtable_events.dtable_io.import_table_from_base import import_table_from_base

        task_id = str(uuid.uuid4())
        task = (import_table_from_base, (context,))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_import_common_dataset_task(self, context):
        from dtable_events.dtable_io.import_sync_common_dataset import import_common_dataset

        task_id = str(uuid.uuid4())
        context['app'] = self.app
        task = (import_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_sync_common_dataset_task(self, context):
        """
        return: task_id -> str or None, error_type -> str or None
        """
        from dtable_events.dtable_io.import_sync_common_dataset import sync_common_dataset

        dataset_sync_id = context.get('sync_id')
        with self.dataset_sync_ids_lock:
            if self.is_syncing(dataset_sync_id):
                return None, 'syncing'
            self.add_dataset_sync(dataset_sync_id)

        task_id = str(uuid.uuid4())
        context['app'] = self.app
        task = (sync_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id, None

    @log_function_call
    def add_force_sync_common_dataset_task(self, context):
        """
        return: task_id -> str or None, error_type -> str or None
        """
        from dtable_events.dtable_io.import_sync_common_dataset import force_sync_common_dataset

        dataset_id = context.get('dataset_id')
        with self.force_sync_dataset_ids_lock:
            if self.is_dataset_force_syncing(dataset_id):
                return None, 'syncing'
            self.force_sync_dataset_ids.add(dataset_id)

        task_id = str(uuid.uuid4())
        context['app'] = self.app
        task = (force_sync_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id, None

    @log_function_call
    def add_convert_view_to_excel_task(self, dtable_uuid, table_id, view_id, username, id_in_org, user_department_ids_map, permission, name, repo_id, is_support_image):
        from dtable_events.dtable_io import convert_view_to_excel

        task_id = str(uuid.uuid4())
        task = (convert_view_to_excel, (dtable_uuid, table_id, view_id, username, id_in_org, user_department_ids_map, permission, name, repo_id, is_support_image))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_convert_table_to_excel_task(self, dtable_uuid, table_id, username, name, repo_id, is_support_image):
        from dtable_events.dtable_io import convert_table_to_excel

        task_id = str(uuid.uuid4())
        task = (convert_table_to_excel, (dtable_uuid, table_id, username, name, repo_id, is_support_image))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_app_users_sync_task(self, dtable_uuid, app_name, app_id, table_name, table_id, username):
        from dtable_events.dtable_io import app_user_sync
        task_id = str(uuid.uuid4())
        task = (app_user_sync, (dtable_uuid, app_name, app_id, table_name, table_id, username, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_export_page_design_task(self, repo_id, dtable_uuid, page_id, username):
        from dtable_events.dtable_io import export_page_design
        task_id = str(uuid.uuid4())
        task = (export_page_design, (repo_id, dtable_uuid, page_id, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_import_page_design_task(self, repo_id, workspace_id, dtable_uuid, page_id, is_dir, username):
        from dtable_events.dtable_io import import_page_design
        task_id = str(uuid.uuid4())
        task = (import_page_design, (repo_id, workspace_id, dtable_uuid, page_id, is_dir, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_export_document_task(self,repo_id, dtable_uuid, doc_uuid, parent_path, filename, username):
        from dtable_events.dtable_io import export_document

        task_id = str(uuid.uuid4())
        task = (export_document,
                (repo_id, dtable_uuid, doc_uuid, parent_path, filename, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)

        return task_id

    @log_function_call
    def add_import_document_task(self, repo_id, dtable_uuid, doc_uuid, view_id, table_id, username):
        from dtable_events.dtable_io import import_document

        task_id = str(uuid.uuid4())
        task = (import_document,
                (repo_id, dtable_uuid, doc_uuid, view_id, table_id, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), 'io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
        return task_id

    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_task(self):
        from dtable_events.dtable_io import dtable_io_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_io_logger.error(e)
                continue

            task = self.tasks_map.get(task_id)
            if type(task) != tuple or len(task) < 1:
                continue
            if type(task[0]).__name__ != 'function':
                continue
            task_info = task_id + ' ' + str(task[0])
            try:
                self.current_task_info[task_id] = task_info
                dtable_io_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task_result = task[0](*task[1])
                if isinstance(task_result, dict):
                    task_result['success'] = True
                else:
                    task_result = {'success': True}
                publish_metric(self.tasks_queue.qsize(), metric_name='io_task_queue_size', metric_help=TASK_MANAGER_METRIC_HELP)
                self.task_results_map[task_id] = task_result

                finish_time = time.time()
                dtable_io_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                self.task_results_map[task_id] = {
                    'success': False,
                    'error_msg': str(e.args[0])
                }
                if str(e.args[0]) in ('Excel format error', 'Number of cells returned exceeds the limit of 1 million', 'base_exceeds_limit'):
                    dtable_io_logger.warning('Failed to handle task %s args: %s error: %s \n' % (task_info, task[1], e))
                elif str(e.args[0]).startswith('import_sync_common_dataset:'):
                    # Errors in import/sync common dataset, those have been record in real task code, so no duplicated error logs here
                    # Including source/destination table not found...
                    dtable_io_logger.warning('Failed to handle task %s args: %s error: %s \n' % (task_info, task[1], e))
                elif str(e.args[0]).startswith('import_table_from_base:'):
                    # Errors in import-table-from-base, those have been record in real task code, so no duplicated error logs here
                    dtable_io_logger.warning('Failed to handle task %s args: %s error: %s \n' % (task_info, task[1], e))
                else:
                    dtable_io_logger.exception(e)
                    dtable_io_logger.error('Failed to handle task %s, args: %s, error: %s \n' % (task_info, task[1], e))
                self.current_task_info.pop(task_id, None)
            finally:
                self.tasks_map.pop(task_id, None)
                if getattr(task[0], '__name__', None) == 'sync_common_dataset':
                    context = task[1][0]
                    self.finish_dataset_sync(context.get('sync_id'))
                if getattr(task[0], '__name__', None) == 'force_sync_common_dataset':
                    context = task[1][0]
                    self.finish_dataset_force_sync(context.get('dataset_id'))

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'TaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            self.threads.append(t)
            t.setDaemon(True)
            t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)

    def is_syncing(self, sync_id):
        return sync_id in self.dataset_sync_ids

    def finish_dataset_sync(self, sync_id):
        with self.dataset_sync_ids_lock:
            self.dataset_sync_ids -= {sync_id}

    def add_dataset_sync(self, db_sync_id):
        self.dataset_sync_ids.add(db_sync_id)

    def is_dataset_force_syncing(self, dataset_id):
        return dataset_id in self.force_sync_dataset_ids

    def finish_dataset_force_sync(self, dataset_id):
        with self.force_sync_dataset_ids_lock:
            self.force_sync_dataset_ids -= {dataset_id}


task_manager = TaskManager()
