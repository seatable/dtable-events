import os
from seaserv import seafile_api
from dtable_events.utils.dtable_storage_server_api import storage_api
from dtable_events.utils.default_dtable import DefaultDtable

FILE_TYPE = '.dtable'
TMP_PATH = '/tmp/storage-backend/'


class StorageBackend(object):
    """Storage Backend
    """

    def __init__(self):
        pass

    def _get_local_file_path(self):
        if not os.path.exists(TMP_PATH):
            os.makedirs(TMP_PATH)
        return TMP_PATH

    def create_empty_dtable(self, dtable_uuid, username, in_storage, lang, repo_id, dtable_file_name):
        json_string = DefaultDtable(username, lang).generate()
        if in_storage:
            return storage_api.save_dtable(dtable_uuid, json_string)
        else:
            dtable_path = os.path.join(self._get_local_file_path(), dtable_uuid)
            with open(dtable_path, 'w') as f:
                f.write(json_string)
            return seafile_api.post_file(repo_id, dtable_path, '/', dtable_file_name, username)

    def save_dtable(self, dtable_uuid, json_string, username, in_storage, repo_id, dtable_file_name):
        if in_storage:
            return storage_api.save_dtable(dtable_uuid, json_string)
        else:
            dtable_path = os.path.join(self._get_local_file_path(), dtable_uuid)
            with open(dtable_path, 'w') as f:
                f.write(json_string)
            return seafile_api.post_file(repo_id, dtable_path, '/', dtable_file_name, username)


storage_backend = StorageBackend()
