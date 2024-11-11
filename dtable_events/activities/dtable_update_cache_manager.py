from dtable_events.utils import uuid_str_to_32_chars
from copy import deepcopy
from threading import Lock

class DTableUpdateCacheManager(object):

    def __init__(self):
        self._updated_time_dict = {}
        self._lock = Lock()

    def set_update_time(self, dtable_uuid, op_time):
        dtable_uuid = uuid_str_to_32_chars(dtable_uuid)
        with self._lock:
            self._updated_time_dict[dtable_uuid] = op_time
    
    @property
    def updated_time_dict(self):
        with self._lock:
            time_dict_copy = deepcopy(self._updated_time_dict)
        return time_dict_copy

    def clean_dtable_update_time_info(self):
        with self._lock:
            self._updated_time_dict = {}
