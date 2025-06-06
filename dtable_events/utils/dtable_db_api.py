import json
import logging
import requests
import jwt
import time
from datetime import datetime
from dtable_events.app.config import DTABLE_PRIVATE_KEY
from dtable_events.utils import uuid_str_to_36_chars

logger = logging.getLogger(__name__)

TIMEOUT = 90

class RowInsertedError(Exception):
    pass

class RowUpdatedError(Exception):
    pass

class RowsQueryError(Exception):
    pass

class RowDeletedError(Exception):
    pass

class Request429Error(Exception):
    pass

def parse_response(response):
    if response.status_code >= 400:
        if response.status_code == 429:
            raise Request429Error()
        try:
            info = response.json()
        except:
            error_msg = response.text
        else:
            error_msg = info.get('error_message')

        raise ConnectionError(response.status_code, error_msg)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


def is_single_multiple_structure(column):
    column_type = column['type']
    if column_type in ('single-select', 'multiple-select'):
        data = column.get('data', {})  # data may be None
        if not data:
            return True, []
        options = data.get('options', [])
        return True, options
    if column_type in ('link', 'link-formula'):
        array_type = column.get('data', {}).get('array_type')
        if array_type in ('single-select', 'multiple-select'):
            data = column.get('data', {})  # data may be None
            if not data:
                return True, []
            array_data = data.get('array_data', {})
            if not array_data:
                return True, []
            options = array_data.get('options', [])
            return True, options
    return False, []


def convert_db_rows(metadata, results):
    """ Convert dtable-db rows data to readable rows data

    :param metadata: list
    :param results: list
    :return: list
    """
    if not results:
        return []
    converted_results = []
    column_map = {column['key']: column for column in metadata}
    select_map = {}
    for column in metadata:
            is_sm_structure, column_options = is_single_multiple_structure(column)
            if is_sm_structure:
                column_data = column['data']
                if not column_data:
                    continue
                column_key = column['key']
                select_map[column_key] = {
                    select['id']: select['name'] for select in column_options}

    for result in results:
        item = {}
        for column_key, value in result.items():
            if column_key in column_map:
                column = column_map[column_key]
                column_name = column['name']
                column_type = column['type']
                s_map = select_map.get(column_key)
                if column_type == 'single-select' and value and s_map:
                    item[column_name] = s_map.get(value, value)
                elif column_type == 'multiple-select' and value and s_map:
                    item[column_name] = [s_map.get(s, s) for s in value]
                elif column_type == 'link' and value and s_map:
                    new_data = []
                    for s in value:
                        old_display_value = s.get('display_value')
                        if isinstance(old_display_value, list):
                            s['display_value'] = old_display_value and [s_map.get(v, v) for v in old_display_value] or []
                        else:
                            s['display_value'] = s_map.get(old_display_value, old_display_value)
                        new_data.append(s)
                    item[column_name] = new_data
                elif column_type == 'link-formula' and value and s_map:
                    if isinstance(value[0], list):
                        item[column_name] = [[s_map.get(v, v) for v in s] for s in value]
                    else:
                        item[column_name] = [s_map.get(s, s) for s in value]

                elif column_type == 'date':
                    try:
                        if value:
                            date_value = datetime.fromisoformat(value)
                            date_format = column['data']['format']
                            if date_format == 'YYYY-MM-DD':
                                value = date_value.strftime('%Y-%m-%d')
                            else:
                                value = date_value.strftime('%Y-%m-%d %H:%M:%S')
                        else:
                            value = None
                    except Exception as e:
                        logger.warning('format date:: %s', e)
                    item[column_name] = value
                else:
                    item[column_name] = value
            else:
                item[column_key] = value
        converted_results.append(item)

    return converted_results


class DTableDBAPI(object):

    def __init__(self, username, dtable_uuid, dtable_db_url):
        self.username = username
        self.dtable_uuid = uuid_str_to_36_chars(dtable_uuid) if dtable_uuid else None
        self.headers = None
        self.dtable_db_url = dtable_db_url.rstrip('/') if dtable_db_url else None
        self._init()

    def _init(self):
        access_token = self.get_dtable_db_token()
        self.headers = {'Authorization': 'Token ' + access_token}
        admin_access_token = self.get_dtable_db_token(is_db_admin=True)
        self.admin_headers = {'Authorization': 'Token ' + admin_access_token}

    def get_dtable_db_token(self, is_db_admin=None):
        payload={
            'exp': int(time.time()) + 3600 * 12 * 24,
            'dtable_uuid': self.dtable_uuid,
            'username': self.username,
            'permission': 'rw',
        }
        if is_db_admin:
            payload['is_db_admin'] = True
        token = jwt.encode(
            payload=payload,
            key=DTABLE_PRIVATE_KEY
        )
        if isinstance(token, bytes):
            token = token.decode()
        return token

    def query(self, sql, convert=True, server_only=True):
        """
        :param sql: str
        :param convert: bool
        :return: list
        """

        if not sql:
            raise ValueError('sql can not be empty.')
        url = self.dtable_db_url + '/api/v1/query/' + self.dtable_uuid + '/?from=dtable_events'
        json_data = {'sql': sql, 'server_only': server_only, 'convert_keys': convert, 'convert_date': convert}
        response = requests.post(url, json=json_data, headers=self.headers, timeout=TIMEOUT)
        data = parse_response(response)
        if not data.get('success'):
            if response.status_code == 200:
                raise RowsQueryError(data.get('error_message'))
            raise Exception(data.get('error_message'))
        metadata = data.get('metadata')
        results = data.get('results')
        return results, metadata

    def insert_rows(self, table_name, rows):
        api_url = "%s/api/v1/insert-rows/%s/?from=dtable_events" % (
            self.dtable_db_url.rstrip('/'),
            self.dtable_uuid
        )

        params = {
            "table_name": table_name,
            "rows": rows
        }
        resp = requests.post(api_url, json=params, headers=self.headers, timeout=TIMEOUT)
        if not resp.status_code == 200:
            logger.error('error insert rows resp: %s', resp.text)
            raise RowInsertedError
        return resp.json()

    def batch_update_rows(self, table_name, rows_data):
        url = "%s/api/v1/update-rows/%s?from=dtable_events" % (
            self.dtable_db_url,
            self.dtable_uuid
        )

        json_data = {
            'table_name': table_name,
            'updates': rows_data,
        }
        resp = requests.put(url, json=json_data, headers=self.headers, timeout=TIMEOUT)
        if not resp.status_code == 200:
            raise RowUpdatedError
        return resp.json()

    def batch_delete_rows(self, table_name, row_ids):
        url = "%s/api/v1/delete-rows/%s?from=dtable_events" % (
            self.dtable_db_url,
            self.dtable_uuid
        )

        json_data = {
            'table_name': table_name,
            'row_ids': row_ids
        }
        resp = requests.delete(url, json=json_data, headers=self.headers, timeout=TIMEOUT)
        if not resp.status_code == 200:
            raise RowDeletedError
        return resp.json()

    def get_metadata(self):
        url = '%s/api/v1/metadata/%s?from=dtable_events' % (
            self.dtable_db_url,
            self.dtable_uuid
        )
        resp = requests.get(url, headers=self.headers, timeout=TIMEOUT)
        return parse_response(resp)

    def add_index(self, table_id, column_names):
        url = '%s/api/v1/index/%s?from=dtable_events' % (
            self.dtable_db_url,
            self.dtable_uuid
        )
        json_data = {
            'table_id': table_id,
            'columns': column_names
        }
        resp = requests.post(url, json=json_data, headers=self.headers, timeout=TIMEOUT)
        return parse_response(resp)

    def list_bases(self, offset=None, limit=None):
        url = '%s/api/v1/bases/' % self.dtable_db_url
        params = {}
        if offset is not None:
            params['offset'] = offset
        if limit is not None:
            params['limit'] = limit
        params['from'] = 'dtable_events'
        resp = requests.get(url, params=params, headers=self.admin_headers)
        return parse_response(resp)
