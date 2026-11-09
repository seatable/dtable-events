"""Buffer and append the current connection's sync log."""
from datetime import datetime
import logging
import time

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError


logger = logging.getLogger(__name__)
RETRY_INTERVAL = 30


class SyncLogger:
    def __init__(self, db_session_class, connection_id):
        self.db_session_class = db_session_class
        self.connection_id = connection_id
        self.log_lines = []
        self.initialized = False
        self.retry_after = 0

    def initialize_log(self):
        try:
            with self.db_session_class() as session:
                session.execute(text('UPDATE connections SET last_sync_log = :details WHERE id = :id'),
                                {'details': '', 'id': self.connection_id})
                session.commit()
            self.initialized = True
        except SQLAlchemyError as error:
            self.retry_after = time.monotonic() + RETRY_INTERVAL
            logger.error('Unable to initialize ScaleDB sync log: connection=%s error_type=%s',
                         self.connection_id, type(error).__name__)
        return self

    def log(self, message):
        if not message:
            return
        message = message.replace('\r', ' ').replace('\n', ' ')
        self.log_lines.append('[%s] %s' % (datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'), message))
        if len(self.log_lines) >= 100 and time.monotonic() >= self.retry_after:
            self.flush()

    def flush(self):
        if not self.log_lines:
            return
        if not self.initialized:
            self.initialize_log()
            if not self.initialized:
                return
        try:
            with self.db_session_class() as session:
                session.execute(text("""UPDATE connections
                                        SET last_sync_log = CONCAT(COALESCE(last_sync_log, ''), :append_text)
                                        WHERE id = :id"""),
                                {'append_text': '\n'.join(self.log_lines) + '\n', 'id': self.connection_id})
                session.commit()
            self.log_lines = []
            self.retry_after = 0
        except SQLAlchemyError as error:
            self.retry_after = time.monotonic() + RETRY_INTERVAL
            logger.error('Unable to append ScaleDB sync log: connection=%s error_type=%s',
                         self.connection_id, type(error).__name__)
