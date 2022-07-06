import uuid
import requests

try:
    from seahub.settings import DTABLE_STORAGE_SERVER_URL
except ImportError as err:
    DTABLE_STORAGE_SERVER_URL = ''


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
        response = requests.put(url, json=json_string, timeout=TIMEOUT)
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


storage_api = DTableStorageServerAPI()
