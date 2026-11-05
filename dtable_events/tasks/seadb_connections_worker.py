"""Consume connection sync tasks."""
import json
import logging
import traceback
from threading import Event, Thread

from sqlalchemy import event

from dtable_events.app.config import SEADB_SERVER_URL, SEADB_USER, SEADB_PASSWORD
from dtable_events.db import init_db_session_class
from dtable_events.seadb_connections.sync_connection import sync_connection
from dtable_events.seadb_connections.sync_manager import QUEUE_NAME, sync_manager


logger = logging.getLogger(__name__)
WORKER_COUNT = 3


def set_connection_charset(dialect, connection_record, connection_args, connection_params):
    connection_params['charset'] = 'utf8mb4'


class SeaDBConnectionsWorker(Thread):
    def __init__(self):
        super().__init__(name='seadb-connections-worker', daemon=True)
        self.db_session_class = None
        self.engine = None
        self.stop_event = Event()

    def start(self):
        if not SEADB_SERVER_URL or not SEADB_USER or not SEADB_PASSWORD:
            logger.warning('ScaleDB connections worker not started: service configuration is incomplete.')
            return
        self.db_session_class = init_db_session_class()
        self.engine = self.db_session_class.kw['bind']
        event.listen(self.engine, 'do_connect', set_connection_charset)
        super().start()

    def cancel(self):
        self.stop_event.set()

    def consume_sync_tasks(self):
        while not self.stop_event.is_set():
            try:
                task = sync_manager.queue_client.brpop(QUEUE_NAME, timeout=1)
                if task is None:
                    continue
                _, message = task
                try:
                    data = json.loads(message)
                except (ValueError, TypeError):
                    logger.warning('Ignoring an invalid ScaleDB sync task.')
                    continue
                self.process_message(data)
            except Exception as error:
                logger.error('ScaleDB sync task processing failed: error_type=%s\n%s',
                             type(error).__name__, ''.join(traceback.format_tb(error.__traceback__)))
                self.stop_event.wait(1)

    def process_message(self, data):
        if self.stop_event.is_set():
            return
        connection_id = data.get('connection_id')
        sync_connection(self.db_session_class, connection_id)

    def run(self):
        workers = [Thread(target=self.consume_sync_tasks, name='seadb-sync-%s' % index, daemon=True)
                   for index in range(WORKER_COUNT)]
        for worker in workers:
            worker.start()
        logger.info('Starting ScaleDB sync worker: concurrency=%s', WORKER_COUNT)
        try:
            self.stop_event.wait()
        finally:
            self.cancel()
            for worker in workers:
                worker.join()
            self.engine.dispose()
            logger.info('ScaleDB sync worker stopped')
