from queue import Queue
from threading import Thread

from dtable_events.dtable_io.utils import get_dtable_server_token
from dtable_events.page_design.utils import convert_page_to_pdf
from dtable_events.utils import uuid_str_to_36_chars

class ConvertTOPDFManager:

    def __init__(self, config):
        self.config = config
        self.queue = Queue(10)  # element in queue is a dict about task

    def do_convert(self):
        while True:
            task_info = self.queue.get()
            dtable_uuid = task_info.get('dtable_uuid')
            page_id = task_info.get('page_id')
            row_id = task_info.get('row_id')
            access_token = get_dtable_server_token('dtable-events', uuid_str_to_36_chars(dtable_uuid))
            convert_page_to_pdf(dtable_uuid, page_id, row_id, access_token)

    def add_task(self, dtable_uuid, page_id, row_id):
        self.queue.put({

         }, block=True)
