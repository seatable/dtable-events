import logging
import os
import queue
import sys
import threading
import time

central_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', '')
dtable_web_service_url = "http://127.0.0.1:8000"
dtable_server_url = "http://127.0.0.1:5000"
if central_conf_dir:
    try:
        if os.path.exists(central_conf_dir):
            sys.path.insert(0, central_conf_dir)
            import dtable_web_settings as seahub_settings
            DTABLE_WEB_SERVICE_URL = getattr(seahub_settings, 'DTABLE_WEB_SERVICE_URL')
            DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
            DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
            ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
            DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
            SESSION_COOKIE_NAME = getattr(seahub_settings, 'SESSION_COOKIE_NAME', 'sessionid')
            dtable_web_service_url = DTABLE_WEB_SERVICE_URL
            dtable_private_key = DTABLE_PRIVATE_KEY
            dtable_server_url = DTABLE_SERVER_URL
            enable_dtable_server_cluster = ENABLE_DTABLE_SERVER_CLUSTER
            dtable_proxy_server_url = DTABLE_PROXY_SERVER_URL
            session_cookie_name = SESSION_COOKIE_NAME
    except ImportError:
        dtable_web_seahub_dir = os.path.join(os.environ.get('DTABLE_WEB_DIR', ''), 'seahub')
        if os.path.exists(dtable_web_seahub_dir):
            sys.path.insert(0, dtable_web_seahub_dir)
            import local_settings as seahub_settings
            DTABLE_WEB_SERVICE_URL = getattr(seahub_settings, 'DTABLE_WEB_SERVICE_URL')
            DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
            DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
            ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
            DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
            SESSION_COOKIE_NAME = getattr(seahub_settings, 'SESSION_COOKIE_NAME', 'sessionid')
            dtable_web_service_url = DTABLE_WEB_SERVICE_URL
            dtable_private_key = DTABLE_PRIVATE_KEY 
            dtable_server_url = DTABLE_SERVER_URL
            enable_dtable_server_cluster = ENABLE_DTABLE_SERVER_CLUSTER
            dtable_proxy_server_url = DTABLE_PROXY_SERVER_URL
            session_cookie_name = SESSION_COOKIE_NAME
    except Exception as e:
        logging.critical(f'import settings from SEAFILE_CENTRAL_CONF_DIR/dtable_web_settings.py failed {e}')
        raise RuntimeError("Can not import dtable_web settings: %s" % e)


class TaskMessageManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_result_map = {}
        self.tasks_queue = queue.Queue(10)
        self.config = None
        self.current_task_info = None
        self.t = None
        self.conf = {
            'dtable_private_key': dtable_private_key,
            'dtable_web_service_url': dtable_web_service_url,
            'dtable_server_url': dtable_server_url
        }

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
