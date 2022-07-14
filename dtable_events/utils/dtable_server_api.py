import json
import logging
import requests
from dtable_events.dtable_io.utils import get_dtable_server_token

logger = logging.getLogger(__name__)

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
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/?from=dtable_events'
        response = requests.get(url, headers=self.headers)
        data = parse_response(response)
        return data.get('metadata')


    def add_table(self, table_name, lang='cn', columns=None):
        logger.debug('add table table_name: %s columns: %s', table_name, columns)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/tables/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'lang': lang,
        }
        if columns:
            json_data['columns'] = columns
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def list_rows(self, table_name):
        logger.debug('list rows table_name: %s', table_name)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        params = {
            'table_name': table_name,
        }
        response = requests.get(url, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('rows')

    def list_columns(self, table_name, view_name=None):
        logger.debug('list columns table_name: %s view_name: %s', table_name, view_name)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/?from=dtable_events'
        params = {'table_name': table_name}
        if view_name:
            params['view_name'] = view_name
        response = requests.get(url, params=params, headers=self.headers)
        data = parse_response(response)
        return data.get('columns')

    def insert_column(self, table_name, column_name, column_type, column_data=None):
        logger.debug('insert column table_name: %s, column_name: %s, column_type: %s, column_data: %s', table_name, column_name, column_type, column_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'column_name': column_name,
            'column_type': column_type
        }
        if column_data:
            json_data['column_data'] = column_data
        response = requests.post(url, json=json_data, headers=self.headers)
        data = parse_response(response)
        return data

    def batch_append_rows(self, table_name, rows_data):
        logger.debug('batch append rows table_name: %s rows_data: %s', table_name, rows_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-append-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def append_row(self, table_name, row_data):
        logger.debug('append row table_name: %s row_data: %s', table_name, row_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row': row_data
        }
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def update_row(self, table_name, row_id, row_data):
        logger.debug('update row table_name: %s row_id: %s row_data: %s', table_name, row_id, row_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
            'row': row_data
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def batch_update_rows(self, table_name, rows_data):
        logger.debug('batch update rows table_name: %s rows_data: %s', table_name, rows_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'updates': rows_data,
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def batch_delete_rows(self, table_name, row_ids):
        logger.debug('batch delete rows table_name: %s row_ids: %s', table_name, row_ids)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-delete-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids,
        }
        response = requests.delete(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def internal_filter_rows(self, json_data):
        """
        for example:
            json_data = {
                'table_id': table_id,
                'filter_conditions': {
                    'filter_groups':filter_groups,
                    'group_conjunction': 'And'
                },
                'limit': 500
            }
        """
        logger.debug('internal filter rows json_data: %s', json_data)
        url = self.dtable_server_url + '/api/v1/internal/dtables/' + self.dtable_uuid + '/filter-rows/?from=dtable_events'
        response = requests.post(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def lock_rows(self, table_name, row_ids):
        logger.debug('lock rows table_name: %s row_ids: %s', table_name, row_ids)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/lock-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def update_link(self, link_id, table_id, other_table_id, row_id, other_rows_ids):
        logger.debug('update links link_id: %s table_id: %s row_id: %s other_table_id: %s other_rows_ids: %s', link_id, table_id, row_id, other_table_id, other_rows_ids)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/links/?from=dtable_events'
        json_data = {
            'row_id': row_id,
            'link_id': link_id,
            'table_id': table_id,
            'other_table_id': other_table_id,
            'other_rows_ids': other_rows_ids
        }
        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)
