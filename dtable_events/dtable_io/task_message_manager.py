import logging
import os
import queue
import sys
import threading
import time

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL


class TaskMessageManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_result_map = {}
        self.tasks_queue = queue.Queue(10)
        self.config = None
        self.current_task_info = None
        self.t = None
        self.conf = {}

    def init(self, workers, file_server_port, io_task_timeout, config):
        self.conf['file_server_port'] = file_server_port
        self.conf['io_task_timeout'] = io_task_timeout
        self.conf['workers'] = workers

        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_map.keys()
    
    def add_email_sending_task(self, auth_info, send_info, username):
        from dtable_events.dtable_io import send_email_msg
        task_id = str(int(time.time() * 1000))
        task = (send_email_msg,(auth_info, send_info, username, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_wechat_sending_task(self, webhook_url, msg ):
        from dtable_events.dtable_io import send_wechat_msg
        task_id = str(int(time.time() * 1000))
        task = (send_wechat_msg, (webhook_url, msg))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_dingtalk_sending_task(self, webhook_url, msg ):
        from dtable_events.dtable_io import send_dingtalk_msg
        task_id = str(int(time.time() * 1000))
        task = (send_dingtalk_msg, (webhook_url, msg))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def query_status(self, task_id):
        task = self.tasks_map[task_id]
        if task == 'success':
            task_result = self.tasks_result_map.get(task_id)
            self.tasks_map.pop(task_id, None)
            self.tasks_result_map.pop(task_id, None)
            return True, task_result
        return False, None

    def handle_task(self):
        from dtable_events.dtable_io import dtable_message_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_message_logger.error(e)
                continue

            try:
                task = self.tasks_map[task_id]
                self.current_task_info = task_id + ' ' + str(task[0])
                dtable_message_logger.info('Run task: %s' % self.current_task_info)
                start_time = time.time()

                # run
                result = task[0](*task[1])
                self.tasks_map[task_id] = 'success'
                self.tasks_result_map[task_id] = result

                finish_time = time.time()
                dtable_message_logger.info('Run task success: %s cost %ds \n' % (self.current_task_info, int(finish_time - start_time)))
                self.current_task_info = None
            except Exception as e:
                dtable_message_logger.error('Failed to handle task %s, error: %s \n' % (task_id, e))
                self.tasks_map.pop(task_id, None)
                self.current_task_info = None

    def run(self):
        t_name = 'MessageTaskManager Thread'
        self.t = threading.Thread(target=self.handle_task, name=t_name)
        self.t.setDaemon(True)
        self.t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)


message_task_manager = TaskMessageManager()
