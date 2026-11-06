"""Execute a connection sync and persist its latest state."""
import json
import logging

import requests
from sqlalchemy import JSON, text
from sqlalchemy.exc import OperationalError

from dtable_events.seadb_connections import SYNC_HANDLERS
from dtable_events.seadb_connections.mapping import InvalidConnection
from dtable_events.seadb_connections.seatable_sync import UpstreamUnavailable
from dtable_events.seadb_connections.sync_logger import SyncLogger


logger = logging.getLogger(__name__)


class SyncProgress:
    def __init__(self, db_session_class, connection):
        self.db_session_class = db_session_class
        self.connection = connection
        self.inserted_count = 0
        self.updated_count = 0

    def save_status_and_time(self, last_sync_time):
        self.connection.status['last_sync_count'] = self.inserted_count + self.updated_count
        with self.db_session_class() as session:
            session.execute(text("""UPDATE connections SET status = :status,
                                    last_sync_time = :last_sync_time WHERE id = :id"""),
                            {'id': self.connection.id, 'status': json.dumps(self.connection.status),
                             'last_sync_time': last_sync_time})
            session.commit()


def get_sync_failure_message(error):
    if isinstance(error, InvalidConnection):
        return str(error)
    if isinstance(error, ConnectionError):
        code = error.args[0] if error.args and isinstance(error.args[0], int) else None
        if code in (400, 401, 403, 404, 409, 422):
            return 'Source or target rejected the request. Check permissions, fields and unique indexes.'
        return 'Source or target is temporarily unavailable. The sync will be retried.'
    if isinstance(error, (requests.RequestException, UpstreamUnavailable, OperationalError)):
        return 'Source or target is temporarily unavailable. The sync will be retried.'
    return 'Check the service logs for details.'


def sync_connection(db_session_class, connection_id):
    with db_session_class() as session:
        sql = text("""SELECT c.id, c.database_uuid, c.name, c.type, c.config, c.modifier,
                             c.last_sync_time, c.status, c.is_active, c.deleted,
                             d.deleted AS database_deleted
                      FROM connections c JOIN seadb_databases d ON d.uuid = c.database_uuid
                      WHERE c.id = :id FOR UPDATE""").columns(config=JSON, status=JSON)
        connection = session.execute(sql, {'id': connection_id}).first()
        if not connection:
            return False
        if connection.deleted or connection.database_deleted:
            return False
        if not connection.is_active:
            return False
        last_sync_status = connection.status['last_sync_status']
        if last_sync_status == 'syncing':
            return False
        connection.status.clear()
        connection.status.update({'last_sync_status': 'syncing', 'last_sync_count': 0})
        session.execute(text('UPDATE connections SET status = :status WHERE id = :id'),
                        {'id': connection_id, 'status': json.dumps(connection.status)})
        session.commit()

    sync_logger = SyncLogger(db_session_class, connection_id)
    progress = SyncProgress(db_session_class, connection)
    sync_logger.initialize_log()
    sync_logger.log('Start sync connection: %s.' % connection_id)

    try:
        sync_handler = SYNC_HANDLERS.get(connection.type)
        if sync_handler is None:
            raise InvalidConnection('Unsupported connection type: %s.' % connection.type)
        last_sync_time = sync_handler(db_session_class, connection, progress)
    except Exception as error:
        message = get_sync_failure_message(error)
        connection.status['last_sync_status'] = 'failed'
        sync_logger.log('Sync failed: %s' % message)
        progress.save_status_and_time(connection.last_sync_time)
        logger.warning('ScaleDB sync failed: connection=%s error_type=%s', connection.id, type(error).__name__)
    else:
        connection.status['last_sync_status'] = 'completed'
        sync_logger.log('Finish sync connection: %s.' % connection_id)
        progress.save_status_and_time(last_sync_time)
        logger.info('ScaleDB sync completed: connection=%s count=%s',
                    connection.id, connection.status['last_sync_count'])
    finally:
        sync_logger.flush()
    return connection.status['last_sync_status']
