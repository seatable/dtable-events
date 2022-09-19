import queue
import threading
import time


class TaskDataSyncManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_queue = queue.Queue(10)
        self.config = None
        self.current_task_info = {}
        self.conf = {}

    def init(self, workers, file_server_port, io_task_timeout, config):
        self.conf['file_server_port'] = file_server_port
        self.conf['io_task_timeout'] = io_task_timeout
        self.conf['workers'] = workers

        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()

    def add_sync_email_task(self, context):
        from dtable_events.dtable_io import email_sync

        task_id = str(int(time.time() * 1000))
        task = (email_sync, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def query_status(self, task_id):
        task = self.tasks_map[task_id]
        if task == 'success':
            self.tasks_map.pop(task_id, None)
            return True
        return False

    def handle_task(self):
        from dtable_events.dtable_io import dtable_data_sync_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_data_sync_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                task_info = task_id + ' ' + str(task[0])
                self.current_task_info[task_id] = task_info
                dtable_data_sync_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'

                finish_time = time.time()
                dtable_data_sync_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                dtable_data_sync_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info.pop(task_id, None)

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'DataSyncTaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            t.setDaemon(True)
            t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


data_sync_task_manager = TaskDataSyncManager()
