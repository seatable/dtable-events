"""Submit connection sync tasks to the queue."""
import json
import time

from dtable_events.app.event_redis import RedisClient


QUEUE_NAME = 'seadb_connection_sync_tasks'
MIN_TASK_SUBMISSION_INTERVAL = 300


class SyncManager:
    def __init__(self):
        self.queue_client = RedisClient(socket_connect_timeout=5, socket_timeout=5)
        self.pending_tasks = {}
        self.next_cleanup_time = 0

    def add_sync_task(self, connection_id):
        self.queue_client.lpush(QUEUE_NAME, json.dumps({'connection_id': connection_id}))

    def check_and_sync(self, connection_id):
        now_time = time.monotonic()
        # Clean up expired submission records at most once per interval.
        if now_time >= self.next_cleanup_time:
            self.pending_tasks = {
                pending_connection_id: submitted_at
                for pending_connection_id, submitted_at in self.pending_tasks.items()
                if now_time - submitted_at < MIN_TASK_SUBMISSION_INTERVAL
            }
            self.next_cleanup_time = now_time + MIN_TASK_SUBMISSION_INTERVAL
        last_submitted_at = self.pending_tasks.get(connection_id)
        if last_submitted_at is not None and now_time - last_submitted_at < MIN_TASK_SUBMISSION_INTERVAL:
            return
        self.add_sync_task(connection_id)
        self.pending_tasks[connection_id] = time.monotonic()


sync_manager = SyncManager()
