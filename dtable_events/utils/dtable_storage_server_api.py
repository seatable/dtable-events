import os
import uuid
import requests

from dtable_events.app.config import DTABLE_STORAGE_SERVER_URL


TIMEOUT = 90


class StorageAPIError(Exception):
    pass


def uuid_str_to_36_chars(dtable_uuid):
    if len(dtable_uuid) == 32:
        return str(uuid.UUID(dtable_uuid))
    else:
        return dtable_uuid


def parse_response(response):
    if response.status_code >= 400:
        raise StorageAPIError(response.status_code, response.text)
    else:
        if response.text:
            return response.json()  # json data
        else:
            return response.text  # empty string ''


class DTableStorageServerAPI(object):
    """DTable Storage Server API
    """

    def __init__(self):
        """
        :param server_url: str
        """
        self.server_url = DTABLE_STORAGE_SERVER_URL.rstrip('/')

    def __str__(self):
        return '<DTable Storage Server API [ %s ]>' % self.server_url

    def get_dtable(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.get(url, timeout=TIMEOUT)
        try:
            data = parse_response(response)
        except StorageAPIError as e:
            if e.args[0] == 404:
                return None
        return data

    def create_empty_dtable(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.put(url, timeout=TIMEOUT)
        data = parse_response(response)
        return data

    def save_dtable(self, dtable_uuid, json_string):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.put(url, data=json_string, timeout=TIMEOUT)
        data = parse_response(response)
        return data

    def delete_dtable(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.delete(url, timeout=TIMEOUT)
        try:
            data = parse_response(response)
        except StorageAPIError as e:
            if e.args[0] == 404:
                return None
        return data

    def get_backup(self, dtable_uuid, version):
        """Return backup content"""
        url = self.server_url + f'/backups/{dtable_uuid}/{version}'
        response = requests.get(url, timeout=TIMEOUT)
        if not response.ok:
            raise ConnectionError(response.status_code, 'get backup failed')
        return response.content

    def get_backup_chunked(self, dtable_uuid, version, file_path):
        temp_path = f'{file_path}.part'
        url = self.server_url + f'/backups/{dtable_uuid}/{version}'
        try:
            resp = requests.get(url, stream=True, timeout=TIMEOUT)
            resp.raise_for_status()

            with open(temp_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            os.rename(temp_path, file_path)

        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def create_backup(self, dtable_uuid, version, file):
        """
        file: bytes or a file-like obj
        """
        url = self.server_url + f'/backups/{dtable_uuid}/{version}'
        resp = requests.put(url, data=file)
        return resp

    def delete_dtable_all_backups(self, dtable_uuid):
        url = self.server_url + f'/backups/{dtable_uuid}'
        response = requests.delete(url)
        return parse_response(response)


storage_api = DTableStorageServerAPI()
