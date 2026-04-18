from threading import Thread
from waitress import serve

from dtable_events.app.config import IO_SERVER_HOST, IO_SERVER_PORT, IO_SERVER_TASK_TIMEOUT, \
    IO_SERVER_WORKERS
from dtable_events.dtable_io.request_handler import app as application
from dtable_events.dtable_io.task_big_data_manager import big_data_task_manager
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager
from dtable_events.dtable_io.task_data_sync_manager import data_sync_task_manager
from dtable_events.dtable_io.task_plugin_email_manager import plugin_email_task_manager


class DTableIOServer(Thread):

    def __init__(self, app):
        Thread.__init__(self)
        self._parse_config()
        self.app = app
        task_manager.init(self.app, self._workers, self._io_task_timeout)
        message_task_manager.init(self._workers, self._io_task_timeout)
        data_sync_task_manager.init(self._workers, self._io_task_timeout)
        plugin_email_task_manager.init(self._workers, self._io_task_timeout)
        big_data_task_manager.init(self._workers, self._io_task_timeout)

        task_manager.run()
        message_task_manager.run()
        data_sync_task_manager.run()
        plugin_email_task_manager.run()
        big_data_task_manager.run()

    def _parse_config(self):
        self._host = IO_SERVER_HOST
        self._port = IO_SERVER_PORT
        self._workers = IO_SERVER_WORKERS
        self._io_task_timeout = IO_SERVER_TASK_TIMEOUT

    def run(self):
        serve(application, host=self._host, port=int(self._port))
