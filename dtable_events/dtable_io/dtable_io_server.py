import logging
import os
import sys
from threading import Thread

from gevent.pywsgi import WSGIServer

from dtable_events.dtable_io.request_handler import app as application
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager


class DTableIOServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)
        task_manager.init(
            self._workers, self._dtable_private_key, self._dtable_web_service_url,
            self._file_server_port, self._dtable_server_url, self._enable_dtable_server_cluster,
            self._dtable_proxy_server_url, self._io_task_timeout, self._session_cookie_name,
            self._dtable_baidu_map_key, self._dtable_google_map_key, config
        )
        message_task_manager.init(
            self._workers, self._dtable_private_key, self._dtable_web_service_url,
            self._file_server_port, self._dtable_server_url,
            self._io_task_timeout, config
        )
        task_manager.run()
        message_task_manager.run()
        self._server = WSGIServer((self._host, int(self._port)), application)

    def _parse_config(self, config):
        if config.has_option('DTABLE-IO', 'host'):
            self._host = config.get('DTABLE-IO', 'host')
        else:
            self._host = '127.0.0.1'

        if config.has_option('DTABLE-IO', 'port'):
            self._port = config.getint('DTABLE-IO', 'port')
        else:
            self._port = '6000'

        if config.has_option('DTABLE-IO', 'workers'):
            self._workers = config.getint('DTABLE-IO', 'workers')
        else:
            self._workers = 3

        if config.has_option('DTABLE-IO', 'io_task_timeout'):
            self._io_task_timeout = config.getint('DTABLE-IO', 'io_task_timeout')
        else:
            self._io_task_timeout = 3600

        if config.has_option('DTABLE-IO', 'file_server_port'):
            self._file_server_port = config.getint('DTABLE-IO', 'file_server_port')
        else:
            self._file_server_port = 8082

        central_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', '')
        self._dtable_web_service_url = "http://127.0.0.1:8000"
        self._dtable_server_url = "http://127.0.0.1:5000"
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
                    DTABLE_BAIDU_MAP_KEY = getattr(seahub_settings, 'DTABLE_BAIDU_MAP_KEY')
                    DTABLE_GOOGLE_MAP_KEY = getattr(seahub_settings, 'DTABLE_GOOGLE_MAP_KEY')

                    self._dtable_web_service_url = DTABLE_WEB_SERVICE_URL
                    self._dtable_private_key = DTABLE_PRIVATE_KEY
                    self._dtable_server_url = DTABLE_SERVER_URL
                    self._enable_dtable_server_cluster = ENABLE_DTABLE_SERVER_CLUSTER
                    self._dtable_proxy_server_url = DTABLE_PROXY_SERVER_URL
                    self._session_cookie_name = SESSION_COOKIE_NAME
                    self._dtable_baidu_map_key = DTABLE_BAIDU_MAP_KEY
                    self._dtable_google_map_key = DTABLE_GOOGLE_MAP_KEY
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
                    DTABLE_BAIDU_MAP_KEY = getattr(seahub_settings, 'DTABLE_BAIDU_MAP_KEY')
                    DTABLE_GOOGLE_MAP_KEY = getattr(seahub_settings, 'DTABLE_GOOGLE_MAP_KEY')

                    self._dtable_web_service_url = DTABLE_WEB_SERVICE_URL
                    self._dtable_private_key = DTABLE_PRIVATE_KEY 
                    self._dtable_server_url = DTABLE_SERVER_URL
                    self._enable_dtable_server_cluster = ENABLE_DTABLE_SERVER_CLUSTER
                    self._dtable_proxy_server_url = DTABLE_PROXY_SERVER_URL
                    self._session_cookie_name = SESSION_COOKIE_NAME
                    self._dtable_baidu_map_key = DTABLE_BAIDU_MAP_KEY
                    self._dtable_google_map_key = DTABLE_GOOGLE_MAP_KEY
            except Exception as e:
                logging.error(f'import settings from SEAFILE_CENTRAL_CONF_DIR/dtable_web_settings.py failed {e}')

    def run(self):
        self._server.serve_forever()
