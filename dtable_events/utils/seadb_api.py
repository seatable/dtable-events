import requests
import base64
import time
from random import choice

from dtable_events.utils import uuid_str_to_36_chars
from dtable_events.utils.constants import VALID_OPTION_TAGS


def parse_response(response):
    if not 200 <= response.status_code < 300:
        raise ConnectionError(response.status_code, response.text)
    # Creating/deleting a base legitimately returns an empty successful body.
    if not response.content:
        return None
    return response.json()


def quote_identifier(identifier):
    return '`%s`' % str(identifier).replace('`', '``')


def get_seadb_table_id(table):
    if not isinstance(table, dict):
        return ''
    return table.get('id') or table.get('_id') or table.get('table_id') or table.get('name') or ''


class SeaDBAPI:
    def __init__(self, server_url, seadb_user, seadb_passwd, timeout=30):
        self.timeout = timeout
        self.server_url = server_url

        self.seadb_user = seadb_user
        self.seadb_passwd = seadb_passwd
        self.headers = None

    def __enter__(self):
        self.gen_headers()
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.headers = None

    def gen_headers(self):
        auth_str = f"{self.seadb_user}:{self.seadb_passwd}"
        b64 = base64.b64encode(auth_str.encode("utf-8")).decode()
        self.headers = {
            "Authorization": f"Basic {b64}"
        }

    def register_user(self, username, passwd, roles=None):
        url = f'{self.server_url}/api/v1/users'
        data = {
            'username': username,
            'password': passwd,
            'roles': roles or ['default_role'],
        }
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def request_base_access_token(self, base_id, permission, exp=None):
        if exp is None:
            exp = int(time.time()) + 86400
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/jwt-token'
        data = {
            'base_id': base_id,
            'username': self.seadb_user,
            'tables': ['*'],
            'permissions': [permission],
            'exp': exp
        }
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response), exp
    
    def create_base(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/base'
        response = requests.post(url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
    
    def delete_base(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/base'
        response = requests.delete(url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
    
    # tables
    
    def create_table(self, base_id, table_name):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/tables'
        params = {
            'table_name': str(table_name)
        }
        response = requests.post(url, headers=self.headers, json=params, timeout=self.timeout)
        return parse_response(response)

    def get_base_info(self, base_id):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/metadata'
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def query_rows(self, base_id, sql, parameters=None, convert_keys=True):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/query'
        data = {
            'sql': sql,
            'parameters': parameters or [],
            'convert_keys': convert_keys,
        }
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
    
    # rows and indexes
    def insert_rows(self, base_id, table_id, rows):
        base_id = uuid_str_to_36_chars(base_id)
        response = requests.post(f'{self.server_url}/api/v1/{base_id}/rows',
                                 json={'table_id': table_id, 'rows': rows},
                                 headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def update_rows(self, base_id, table_id, updates):
        base_id = uuid_str_to_36_chars(base_id)
        response = requests.put(f'{self.server_url}/api/v1/{base_id}/rows',
                                json={'table_id': table_id, 'updates': updates},
                                headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def create_unique_index(self, base_id, table_id, column_name):
        base_id = uuid_str_to_36_chars(base_id)
        response = requests.post(f'{self.server_url}/api/v1/{base_id}/index',
                                 json={'table_id': table_id, 'columns': [column_name], 'unique': True, 'sync': True},
                                 headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def add_column_option(self, base_id, table_id, column_name, option_name):
        base_id = uuid_str_to_36_chars(base_id)
        color = choice(VALID_OPTION_TAGS)
        data = {
            'table_id': table_id, 'column_name': column_name, 'option_name': option_name,
            'option_data': {'color': color['color'], 'textColor': color['text_color'], 'borderColor': color['border_color']},
        }
        response = requests.post(f'{self.server_url}/api/v1/{base_id}/column-options',
                                 json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def add_column(self, base_id, table_id, column):
        base_id = uuid_str_to_36_chars(base_id)
        url = f'{self.server_url}/api/v1/{base_id}/columns'
        data = {
            'table_id': table_id,
            'column_name': column['column_name'],
            'column_type': column['column_type'],
        }
        if column.get('column_data'):
            data['column_data'] = column['column_data']
        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)
