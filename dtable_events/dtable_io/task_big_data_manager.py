import time
import queue
import threading

class BigDataTaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_status_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None
        self.current_task_info = None
        self.t = None
        self.threads = []
        self.conf = {}

    def init(self, workers, file_server_port, io_task_timeout, config):
        self.conf['file_server_port'] = file_server_port
        self.conf['io_task_timeout'] = io_task_timeout
        self.conf['workers'] = workers

        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()

    def query_status(self, task_id):
        task_status_result = self.tasks_status_map.get(task_id, {})
        if task_status_result.get('status') in ('success', 'terminated'):
            self.tasks_map.pop(task_id, None)
            self.tasks_status_map.pop(task_id, None)
            return True, task_status_result

        return False, task_status_result

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
                self.current_task_info = task_id + ' ' + str(task[0])
                dtable_io_logger.info('Run task: %s' % self.current_task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_io_logger.info(
                    'Run task success: %s cost %ds \n' % (self.current_task_info, int(finish_time - start_time)))
                self.current_task_info = None
            except Exception as e:
                dtable_io_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info = None

    def add_import_big_excel_task(self, username, dtable_uuid, table_name, file_path):
        from dtable_events.dtable_io import import_big_excel
        task_id = str(int(time.time()*1000))
        task = (import_big_excel,
                (username, dtable_uuid, table_name, file_path, task_id, self.tasks_status_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_big_excel_task(self, username, dtable_uuid, table_name, file_path, ref_columns, is_insert_new_data=False):
        from dtable_events.dtable_io import update_big_excel
        task_id = str(int(time.time()*1000))
        task = (update_big_excel,
                (username, dtable_uuid, table_name, file_path, ref_columns, is_insert_new_data, task_id, self.tasks_status_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_convert_big_data_view_to_execl_task(self, dtable_uuid, table_id, view_id, username, name):
        from dtable_events.dtable_io import convert_big_data_view_to_execl

        task_id = str(int(time.time()*1000))
        task = (convert_big_data_view_to_execl,
                (dtable_uuid, table_id, view_id, username, name, task_id, self.tasks_status_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'BigDataTaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            self.threads.append(t)
            t.setDaemon(True)
            t.start()

big_data_task_manager = BigDataTaskManager()
