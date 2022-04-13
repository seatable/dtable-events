import time
import os
import queue
import threading


from seaserv import seafile_api


class TaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None
        self.current_task_info = {}
        self.threads = []

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, dtable_server_url, enable_dtable_server_cluster, dtable_proxy_server_url, io_task_timeout, session_cookie_name, config):
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'dtable_server_url': dtable_server_url,
            'enable_dtable_server_cluster': enable_dtable_server_cluster,
            'dtable_proxy_server_url': dtable_proxy_server_url,
            'io_task_timeout': io_task_timeout,
            'workers': workers,
            'session_cookie_name': session_cookie_name
        }
        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()

    def add_export_task(self, username, repo_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import get_dtable_export_content

        asset_dir_path = os.path.join('/asset', dtable_uuid)
        asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)

        task_id = str(int(time.time()*1000))
        task = (get_dtable_export_content,
                (username, repo_id, dtable_uuid, asset_dir_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage):
        from dtable_events.dtable_io import post_dtable_import_files

        task_id = str(int(time.time()*1000))
        task = (post_dtable_import_files,
                (username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_export_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map=None):
        from dtable_events.dtable_io import get_dtable_export_asset_files

        task_id = str(int(time.time()*1000))
        task = (get_dtable_export_asset_files,
                (username, repo_id, dtable_uuid, files, task_id, files_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_transfer_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map, parent_dir, relative_path, replace, repo_api_token, seafile_server_url):
        from dtable_events.dtable_io import get_dtable_transfer_asset_files
        task_id = str(int(time.time() * 1000))
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
                 seafile_server_url))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_parse_excel_task(self, username, repo_id, workspace_id, dtable_name, custom):
        from dtable_events.dtable_io import parse_excel

        task_id = str(int(time.time()*1000))
        task = (parse_excel,
                (username, repo_id, workspace_id, dtable_name, custom, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import import_excel

        task_id = str(int(time.time()*1000))
        task = (import_excel,
                (username, repo_id, workspace_id, dtable_uuid, dtable_name, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_add_table_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name):
        from dtable_events.dtable_io import import_excel_add_table

        task_id = str(int(time.time()*1000))
        task = (import_excel_add_table,
                (username, repo_id, workspace_id, dtable_uuid, dtable_name, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_append_excel_append_parsed_file_task(self, username, repo_id, dtable_uuid, file_name, table_name):
        from dtable_events.dtable_io import append_excel_append_parsed_file

        task_id = str(int(time.time()*1000))
        task = (append_excel_append_parsed_file,
                (username, repo_id, dtable_uuid, file_name, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_append_excel_upload_excel_task(self, username, repo_id, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import append_excel_upload_excel

        task_id = str(int(time.time()*1000))
        task = (append_excel_upload_excel,
                (username, repo_id, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_run_auto_rule_task(self, username, org_id, dtable_uuid, run_condition, trigger, actions):
        from dtable_events.automations.auto_rules_utils import run_auto_rule_task
        task_id = str(int(time.time() * 1000))
        options = {
            'run_condition': run_condition,
            'dtable_uuid': dtable_uuid,
            'org_id': org_id,
            'creator': username,
        }

        task = (run_auto_rule_task, (trigger, actions, options, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_excel_csv_update_parsed_file_task(self, username, repo_id, dtable_uuid, file_name, table_name,
                                                     selected_columns):
        from dtable_events.dtable_io import update_excel_csv_update_parsed_file

        task_id = str(int(time.time() * 1000))
        task = (update_excel_csv_update_parsed_file,
                (username, repo_id, dtable_uuid, file_name, table_name, selected_columns))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_excel_upload_excel_task(self, username, repo_id, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_excel_upload_excel

        task_id = str(int(time.time() * 1000))
        task = (update_excel_upload_excel,
                (username, repo_id, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_csv_upload_csv_task(self, username, repo_id, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_csv_upload_csv

        task_id = str(int(time.time() * 1000))
        task = (update_csv_upload_csv,
                (username, repo_id, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def query_status(self, task_id):
        task = self.tasks_map[task_id]
        if task == 'success':
            self.tasks_map.pop(task_id, None)
            return True, None
        if isinstance(task, str) and task.startswith('error_'):
            self.tasks_map.pop(task_id, None)
            return True, task[6:]
        return False, None

    def convert_page_to_pdf(self, dtable_uuid, page_id, row_id, access_token, session_id):
        from dtable_events.dtable_io import convert_page_to_pdf

        task_id = str(int(time.time()*1000))
        task = (convert_page_to_pdf,
                (dtable_uuid, page_id, row_id, access_token, session_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_common_dataset_task(self, context):
        from dtable_events.dtable_io.import_sync_common_dataset import import_common_dataset

        task_id = str(int(time.time()*1000))
        task = (import_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_sync_common_dataset_task(self, context):
        from dtable_events.dtable_io.import_sync_common_dataset import sync_common_dataset

        task_id = str(int(time.time()*1000))
        task = (sync_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_convert_view_to_execl_task(self, dtable_uuid, table_id, view_id, username, id_in_org, permission, name,):
        from dtable_events.dtable_io import convert_view_to_execl

        task_id = str(int(time.time()*1000))
        task = (convert_view_to_execl, (dtable_uuid, table_id, view_id, username, id_in_org, permission, name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_convert_table_to_execl_task(self, dtable_uuid, table_id, username, permission, name,):
        from dtable_events.dtable_io import convert_table_to_execl

        task_id = str(int(time.time()*1000))
        task = (convert_table_to_execl, (dtable_uuid, table_id, username, permission, name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

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

            try:
                task = self.tasks_map[task_id]
                task_info = task_id + ' ' + str(task[0])
                self.current_task_info[task_id] = task_info
                dtable_io_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_io_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                dtable_io_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map[task_id] = 'error_' + str(e.args[0])
                self.current_task_info.pop(task_id, None)

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


task_manager = TaskManager()
