"""Find connections needing synchronization and publish their tasks."""
import logging
from threading import Event, Thread

from sqlalchemy import bindparam, text

from dtable_events.app.config import SEADB_SERVER_URL, SEADB_USER, SEADB_PASSWORD
from dtable_events.db import init_db_session_class
from dtable_events.seadb_connections import SYNC_HANDLERS


logger = logging.getLogger(__name__)
SCAN_INTERVAL = 60
SYNC_INTERVAL = 600


class SyncTaskPublisher(Thread):
    def __init__(self, sync_manager):
        super().__init__(name='seadb-sync-task-publisher', daemon=True)
        self.sync_manager = sync_manager
        self.db_session_class = None
        self.engine = None
        self.stop_event = Event()

    def start(self):
        if not SEADB_SERVER_URL or not SEADB_USER or not SEADB_PASSWORD:
            logger.warning('ScaleDB sync task publisher not started: service configuration is incomplete.')
            return
        self.db_session_class = init_db_session_class()
        self.engine = self.db_session_class.kw['bind']
        super().start()

    def cancel(self):
        self.stop_event.set()

    def get_sync_connections_by_page(self, start, limit):
        sql = """
        SELECT c.id
        FROM connections c
        JOIN seadb_databases d ON d.uuid = c.database_uuid
        WHERE c.deleted = 0 AND c.is_active = 1 AND d.deleted = 0
          AND c.type IN :connection_types
          AND (JSON_UNQUOTE(JSON_EXTRACT(c.status, '$.last_sync_status')) = 'pending' OR
               c.last_sync_time IS NULL OR
               TIMESTAMPDIFF(SECOND, c.last_sync_time, UTC_TIMESTAMP()) > :sync_interval)
        ORDER BY c.id
        LIMIT :start, :limit
        """
        statement = text(sql).bindparams(bindparam('connection_types', expanding=True))
        with self.db_session_class() as session:
            return session.execute(statement, {
                'connection_types': list(SYNC_HANDLERS),
                'sync_interval': SYNC_INTERVAL,
                'start': start,
                'limit': limit,
            }).fetchall()

    def publish_sync_tasks(self):
        start, limit = 0, 1000
        while not self.stop_event.is_set():
            try:
                connections = self.get_sync_connections_by_page(start, limit)
            except Exception as error:
                logger.error('Unable to get sync connections: %s', error)
                return
            start += limit
            if not connections:
                break
            for connection in connections:
                if self.stop_event.is_set():
                    return
                connection_id = connection.id
                try:
                    self.sync_manager.check_and_sync(connection_id)
                except Exception as error:
                    logger.error('Unable to submit sync task: connection=%s error=%s', connection_id, error)

    def run(self):
        try:
            while not self.stop_event.is_set():
                try:
                    self.publish_sync_tasks()
                except Exception as error:
                    logger.error('Unable to publish connection sync tasks: %s', error)
                self.stop_event.wait(SCAN_INTERVAL)
        finally:
            self.engine.dispose()
