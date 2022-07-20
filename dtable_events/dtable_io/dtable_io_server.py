from threading import Thread

from gevent.pywsgi import WSGIServer

from dtable_events.dtable_io.request_handler import app as application
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager


class DTableIOServer(Thread):

    def __init__(self, config):
        Thread.__init__(self)
        self._parse_config(config)
        task_manager.init(self._workers, self._file_server_port, self._io_task_timeout, config)
        message_task_manager.init(self._workers, self._file_server_port, self._io_task_timeout, config)
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

    def run(self):
        self._server.serve_forever()
