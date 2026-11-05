"""Handlers write data and update progress; orchestration owns status and checkpoints."""
from dtable_events.seadb_connections.seatable_sync import sync_seatable_connection


# Handlers receive (db_session_class, connection, progress) and return the sync cutoff.
SYNC_HANDLERS = {
    'seatable': sync_seatable_connection,
}
