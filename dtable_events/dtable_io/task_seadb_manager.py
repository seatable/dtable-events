import queue
import threading
import time
import uuid


class SeaDBTaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.tasks_status_map = {}
        self.tasks_queue = queue.Queue(10)
        self.conf = {}

    def init(self, workers, io_task_timeout):
        self.conf['workers'] = workers
        self.conf['io_task_timeout'] = io_task_timeout

    def is_valid_task_id(self, task_id):
        return task_id in self.tasks_status_map

    def query_status(self, task_id):
        task_status_result = self.tasks_status_map.get(task_id, {})
        if task_status_result.get('status') in ('success', 'failed'):
            self.tasks_status_map.pop(task_id, None)
            return True, task_status_result
        return False, task_status_result

    def add_export_task(self, payload):
        from dtable_events.dtable_io.seadb_export import export_seadb_view_to_excel

        task_id = str(uuid.uuid4())
        timeout = self.conf['io_task_timeout']
        self.tasks_status_map[task_id] = {
            'status': 'queued',
            'handled_row_count': 0,
            'filename': payload['filename'],
        }
        self.tasks_map[task_id] = (export_seadb_view_to_excel, (payload, task_id, self.tasks_status_map, timeout))
        try:
            self.tasks_queue.put_nowait(task_id)
        except queue.Full:
            self.tasks_map.pop(task_id, None)
            self.tasks_status_map.pop(task_id, None)
            raise
        return task_id

    def handle_tasks(self):
        from dtable_events.dtable_io import dtable_io_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_io_logger.error(e)
                continue

            try:
                task = self.tasks_map.pop(task_id, None)
                if not isinstance(task, tuple):
                    continue
                dtable_io_logger.info('Run ScaleDB export task: %s', task_id)
                start_time = time.time()
                task[0](*task[1])
                dtable_io_logger.info('Run ScaleDB export task %s cost %ds', task_id, int(time.time() - start_time))
            except Exception as e:
                dtable_io_logger.exception('Failed to handle ScaleDB export task %s: %s', task_id, e)
                self.tasks_status_map.pop(task_id, None)
            finally:
                self.tasks_queue.task_done()

    def run(self):
        for index in range(self.conf['workers']):
            threading.Thread(
                target=self.handle_tasks, name=f'SeaDBTaskManager-{index + 1}', daemon=True,
            ).start()


seadb_task_manager = SeaDBTaskManager()
