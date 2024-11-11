from dtable_events.utils import uuid_str_to_32_chars

class DTableUpdateCacheManager(object):

    def __init__(self):
        self.updated_time_dict = {}

    def set_update_time(self, dtable_uuid, op_time):
        dtable_uuid = uuid_str_to_32_chars(dtable_uuid)
        self.updated_time_dict[dtable_uuid] = op_time
    
    def get_dtable_update_time_info(self):
        return self.updated_time_dict

    def clean_dtable_update_time_info(self):
        self.updated_time_dict = {}
