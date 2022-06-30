import json
import requests
from dtable_events.dtable_io.utils import get_dtable_server_token

def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass
        

class DTableServerAPI(object):
    # simple version of python sdk without authorization for base or table manipulation

    def __init__(self, username, dtable_uuid, dtable_server_url):
        self.username = username
        self.dtable_uuid = dtable_uuid
        self.headers = None
        self.dtable_server_url = dtable_server_url.rstrip('/')
        self._init()

    def _init(self):
        dtable_server_access_token = get_dtable_server_token(self.username, self.dtable_uuid)
        self.headers = {'Authorization': 'Token ' + dtable_server_access_token}

    def get_metadata(self):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'
        response = requests.get(url, headers=self.headers)
        data = parse_response(response)
        return data.get('metadata')


    def add_table(self, table_name, lang='cn', columns=None):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/tables/'
        json_data = {
            'table_name': table_name,
            'lang': lang,
        }
        if columns:
            json_data['columns'] = columns
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def list_rows(self, table_name):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/'
        params = {
            'table_name': table_name,
        }
        response = requests.get(url, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('rows')

    def insert_column(self, table_name, column_name, column_type):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'column_type': column_type
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        data = parse_response(response)
        return data

    def batch_append_rows(self,table_name, rows_data):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-append-rows/'
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def batch_update_rows(self, table_name, rows_data):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-rows/'
        json_data = {
            'table_name': table_name,
            'updates': rows_data,
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def batch_delete_rows(self, table_name, row_ids):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-delete-rows/'
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids,
        }
        response = requests.delete(url, json=json_data, headers=self.headers)
        return parse_response(response)
