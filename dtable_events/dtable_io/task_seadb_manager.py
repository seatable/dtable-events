import queue
import shutil
import threading
import time

from dtable_events.dtable_io import dtable_io_logger, seadb_export


class SeaDBTaskManager:
    def __init__(self):
        self.tasks_queue = queue.Queue(maxsize=10)
        self._cleanup_lock = threading.Lock()
        self._next_cleanup = 0
        self.conf = {}

    def init(self, workers, io_task_timeout):
        self.conf['workers'] = workers
        self.conf['io_task_timeout'] = io_task_timeout

    def add_export_task(self, payload):
        task_id, directory, state = seadb_export.create_job(payload, timeout=self.conf['io_task_timeout'])
        try:
            self.tasks_queue.put_nowait((directory, state, payload))
        except queue.Full:
            shutil.rmtree(directory, ignore_errors=True)
            raise
        return task_id

    def _clean_expired_jobs(self):
        with self._cleanup_lock:
            now = time.monotonic()
            if now < self._next_cleanup:
                return
            self._next_cleanup = now + 300

        try:
            seadb_export.clean_expired_jobs()
        except OSError as error:
            dtable_io_logger.warning('Could not clean ScaleDB exports (%s)', type(error).__name__)

    def handle_tasks(self):
        while True:
            self._clean_expired_jobs()
            try:
                directory, state, payload = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue

            try:
                seadb_export.export_job(directory, state, payload)
            except Exception as error:
                dtable_io_logger.error('Could not finalize ScaleDB export %s (%s)', directory.name, type(error).__name__)
            finally:
                self.tasks_queue.task_done()

    def run(self):
        for index in range(self.conf['workers']):
            threading.Thread(
                target=self.handle_tasks, name=f'SeaDBTaskManager-{index + 1}', daemon=True,
            ).start()


seadb_task_manager = SeaDBTaskManager()
