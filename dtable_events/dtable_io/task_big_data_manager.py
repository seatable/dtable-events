import time
import queue
import threading

class TaskBigDataManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_status_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = None
        self.config = None
        self.current_task_info = None
        self.t = None
        self.threads = []

    def init(self, workers, dtable_private_key, dtable_web_service_url, file_server_port, dtable_server_url,
             io_task_timeout, config):
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'file_server_port': file_server_port,
            'dtable_server_url': dtable_server_url,
            'io_task_timeout': io_task_timeout,
            'workers': workers,
        }
        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()

    def is_valid_task_id_for_status(self, task_id):
        return task_id in self.tasks_status_map.keys()

    def query_status(self, task_id):
        task_status_result = self.tasks_status_map.get(task_id)
        if task_status_result == 'done':
            self.tasks_status_map.pop(task_id)
            return True

        return False

    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_task(self):
        from dtable_events.dtable_io import dtable_big_data_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_big_data_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                self.current_task_info = task_id + ' ' + str(task[0])
                dtable_big_data_logger.info('Run task: %s' % self.current_task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_big_data_logger.info(
                    'Run task success: %s cost %ds \n' % (self.current_task_info, int(finish_time - start_time)))
                self.current_task_info = None
                self.tasks_map.pop(task_id, None)
            except Exception as e:
                dtable_big_data_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info = None

    def add_import_big_excel_task(self, username, dtable_uuid, table_name, file_name, start_row, request_entity, data_binary):
        from dtable_events.dtable_io import import_big_excel
        task_id = str(int(time.time()*1000))
        task = (import_big_excel,
                (username, dtable_uuid, table_name, file_name, start_row, request_entity, data_binary, self.config, task_id, self.tasks_status_map))
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

    def cancel_task(self, task_id):
        self.tasks_status_map[task_id] = 'cancelled'

big_data_task_manager = TaskBigDataManager()
