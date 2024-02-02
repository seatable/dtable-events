import os
from dtable_events.utils.dtable_storage_server_api import storage_api

FILE_TYPE = '.dtable'
TMP_PATH = '/tmp/storage-backend/'
NOT_IN_STORAGE_ERROR_MSG = 'This Base needs to be migrated to storage-server. Please use "/templates/migrate_bases.sh" to migrate Bases to storage-server.'


class StorageBackend(object):
    """Storage Backend
    """

    def __init__(self):
        pass

    def _get_local_file_path(self):
        if not os.path.exists(TMP_PATH):
            os.makedirs(TMP_PATH)
        return TMP_PATH

    def create_empty_dtable(self, dtable_uuid, username, in_storage, repo_id, dtable_file_name):
        if in_storage:
            return storage_api.create_empty_dtable(dtable_uuid)
        else:
            raise RuntimeError(NOT_IN_STORAGE_ERROR_MSG)

    def save_dtable(self, dtable_uuid, json_string, username, in_storage, repo_id, dtable_file_name):
        if in_storage:
            return storage_api.save_dtable(dtable_uuid, json_string)
        else:
            raise RuntimeError(NOT_IN_STORAGE_ERROR_MSG)


storage_backend = StorageBackend()
