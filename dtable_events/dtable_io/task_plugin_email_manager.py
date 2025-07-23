import uuid
import queue
import threading
import time

from dtable_events.utils.utils_metric import publish_metric, PLUGIN_EMAIL_TASK_MANAGER_METRIC_HELP


class TaskPluginEmailManager(object):

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

    def add_send_email_task(self, context):
        from dtable_events.dtable_io import plugin_email_send_email

        task_id = str(uuid.uuid4())
        task = (plugin_email_send_email, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        publish_metric(self.tasks_queue.qsize(), metric_name='plugin_email_io_task_queue_size', metric_help=PLUGIN_EMAIL_TASK_MANAGER_METRIC_HELP)

        return task_id

    def query_status(self, task_id):
        task = self.tasks_map[task_id]
        if task == 'success':
            self.tasks_map.pop(task_id, None)
            return True
        return False

    def handle_task(self):
        from dtable_events.dtable_io import dtable_plugin_email_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_plugin_email_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                if type(task[0]).__name__ != 'function':
                    continue

                task_info = task_id + ' ' + str(task[0])
                self.current_task_info[task_id] = task_info
                dtable_plugin_email_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.tasks_map[task_id] = 'success'
                publish_metric(self.tasks_queue.qsize(), metric_name='plugin_email_io_task_queue_size', metric_help=PLUGIN_EMAIL_TASK_MANAGER_METRIC_HELP)

                finish_time = time.time()
                dtable_plugin_email_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                dtable_plugin_email_logger.exception(e)
                dtable_plugin_email_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info.pop(task_id, None)

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'PluginEmailTaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            t.setDaemon(True)
            t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


plugin_email_task_manager = TaskPluginEmailManager()
