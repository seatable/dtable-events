import uuid
import queue
import threading
import time
from dtable_events.utils.email_sender import ThirdPartyAccountNotFound, ThirdPartyAccountInvalid, \
    ThirdPartyAccountAuthorizationFailure, ThirdPartyAccountFetchTokenFailure, ThirdPartyAccountFetchEmailBoxFailure, \
    InvalidEmailMessage, SendEmailFailure

class TaskPluginEmailManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_result_map = {}
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

                try:
                    result = task[0](*task[1])
                except Exception as e:
                    result = {}
                    if isinstance(e, ThirdPartyAccountNotFound):
                        result['status_code'] = 400
                        result['err_msg'] = 'Third-party account not found'
                    elif isinstance(e, ThirdPartyAccountInvalid):
                        result['status_code'] = 400
                        result['err_msg'] = 'Third-party account is invalid'
                    elif isinstance(e, ThirdPartyAccountAuthorizationFailure):
                        result['status_code'] = 400
                        result['err_msg'] = 'Third-party account authorization failure'
                    elif isinstance(e, ThirdPartyAccountFetchTokenFailure):
                        result['status_code'] = 400
                        result['err_msg'] = 'Third-party account fetches token failure'
                    elif isinstance(e, ThirdPartyAccountFetchEmailBoxFailure):
                        result['status_code'] = 400
                        result['err_msg'] = 'Third-party account fetches sender from mail box failure or the mail box is invalid'
                    elif isinstance(e, InvalidEmailMessage):
                        result['status_code'] = 400
                        result['err_msg'] = 'Invalid email message'
                    elif isinstance(e, SendEmailFailure):
                        result['status_code'] = 400
                        result['err_msg'] = 'Send email failure'
                    else:
                        raise e
                finally:
                    self.tasks_map[task_id] = 'success'
                    self.tasks_result_map[task_id] = result

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
