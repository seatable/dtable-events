import json
import logging
import requests
import io
import os
from urllib import parse
from uuid import UUID
from datetime import datetime
from seaserv import seafile_api
from dtable_events.dtable_io.utils import get_dtable_server_token
from dtable_events.app.config import FILE_SERVER_ROOT

logger = logging.getLogger(__name__)

UPLOAD_IMG_RELATIVE_PATH = 'images'
UPLOAD_FILE_RELATIVE_PATH = 'files'


class WrongFilterException(Exception):
    pass


def parse_response(response):
    if response.status_code >= 400:
        try:
            response_json = response.json()
        except:
            pass
        else:
            if response_json.get('error_type') == 'wrong_filter_in_filters':
                raise WrongFilterException()
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


def is_single_multiple_structure(column):
    column_type = column['type']
    if column_type in ('single-select', 'multiple-select'):
        options = column.get('data', {}).get('options', [])
        return True, options
    if column_type in ('link', 'link-formula'):
        array_type = column.get('data', {}).get('array_type')
        if array_type in ('single-select', 'multiple-select'):
            options = column.get('data', {}).get('array_data', {}).get('options', [])
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
                        print('[Warning] format date:', e)
                    item[column_name] = value
                else:
                    item[column_name] = value
            else:
                item[column_key] = value
        converted_results.append(item)

    return converted_results


def get_fileserver_root():
    """ Construct seafile fileserver address and port.

    Returns:
    	Constructed fileserver root.
    """
    return FILE_SERVER_ROOT.rstrip('/') if FILE_SERVER_ROOT else ''


def gen_file_upload_url(token, op, replace=False):
    url = '%s/%s/%s' % (get_fileserver_root(), op, token)
    if replace is True:
        url += '?replace=1'
    return url


class DTableServerAPI(object):
    # simple version of python sdk without authorization for base or table manipulation

    def __init__(self, username, dtable_uuid, dtable_server_url, server_url=None, dtable_db_url=None, repo_id=None, workspace_id=None):
        self.username = username
        self.dtable_uuid = dtable_uuid
        self.headers = None
        self.dtable_server_url = dtable_server_url.rstrip('/')
        self.server_url = server_url.rstrip('/') if server_url else None
        self.dtable_db_url = dtable_db_url.rstrip('/') if dtable_db_url else None
        self.repo_id = repo_id
        self.workspace_id = workspace_id
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

    def query(self, sql, convert=True):
        """
        :param sql: str
        :param convert: bool
        :return: list
        """
        if not sql:
            raise ValueError('sql can not be empty.')
        url = self.dtable_db_url + '/api/v1/query/' + self.dtable_uuid + '/?from=dtable_events'
        json_data = {'sql': sql}
        response = requests.post(url, json=json_data, headers=self.headers)
        data = parse_response(response)
        if not data.get('success'):
            raise Exception(data.get('error_message'))
        metadata = data.get('metadata')
        results = data.get('results')
        if convert:
            converted_results = convert_db_rows(metadata, results)
            return converted_results
        else:
            return results

    def get_column_link_id(self, table_name, column_name, view_name=None):
        columns = self.list_columns(table_name, view_name)
        for column in columns:
            if column.get('name') == column_name and column.get('type') == 'link':
                return column.get('data', {}).get('link_id')
        raise ValueError('link type column "%s" does not exist in current view' % column_name)

    def batch_update_links(self, link_id, table_id, other_table_id, row_id_list, other_rows_ids_map):
        """
        :param link_id: str
        :param table_id: str
        :param other_table_id: str
        :param row_id_list: []
        :param other_rows_ids_map: dict
        """
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-links/?from=dtable_events'
        json_data = {
            'link_id': link_id,
            'table_id': table_id,
            'other_table_id': other_table_id,
            'row_id_list': row_id_list,
            'other_rows_ids_map': other_rows_ids_map,
        }

        response = requests.put(url, json=json_data, headers=self.headers)
        return parse_response(response)

    def get_file_upload_link(self):
        """
        :return: dict
        """
        repo_id = self.repo_id
        asset_dir_path = '/asset/' + self.dtable_uuid
        asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)
        if not asset_dir_id:
            seafile_api.mkdir_with_parents(repo_id, '/', asset_dir_path[1:], self.username)

        # get token
        obj_id = json.dumps({'parent_dir': asset_dir_path})
        token = seafile_api.get_fileserver_access_token(repo_id, obj_id, 'upload', '', use_onetime=False)

        upload_link = gen_file_upload_url(token, 'upload-api')

        res = dict()
        res['upload_link'] = upload_link
        res['parent_path'] = asset_dir_path
        res['img_relative_path'] = os.path.join(UPLOAD_IMG_RELATIVE_PATH, str(datetime.today())[:7])
        res['file_relative_path'] = os.path.join(UPLOAD_FILE_RELATIVE_PATH, str(datetime.today())[:7])
        return res

    def upload_bytes_file(self, name, content: bytes, relative_path=None, file_type=None, replace=False):
        """
        relative_path: relative path for upload, if None, default {file_type}s/{date of this month} eg: files/2020-09
        file_type: if relative is None, file type must in ['image', 'file'], default 'file'
        return: info dict of uploaded file
        """
        upload_link_dict = self.get_file_upload_link()
        parent_dir = upload_link_dict['parent_path']
        upload_link = upload_link_dict['upload_link'] + '?ret-json=1'
        if not relative_path:
            if file_type and file_type not in ['image', 'file']:
                raise Exception('relative or file_type invalid.')
            if not file_type:
                file_type = 'file'
            relative_path = '%ss/%s' % (file_type, str(datetime.today())[:7])
        else:
            relative_path = relative_path.strip('/')
        response = requests.post(upload_link, data={
            'parent_dir': parent_dir,
            'relative_path': relative_path,
            'replace': 1 if replace else 0
        }, files={
            'file': (name, io.BytesIO(content))
        }, timeout=120)

        d = response.json()[0]
        url = '%(server)s/workspace/%(workspace_id)s/asset/%(dtable_uuid)s/%(relative_path)s/%(filename)s' % {
            'server': self.server_url.strip('/'),
            'workspace_id': self.workspace_id,
            'dtable_uuid': str(UUID(self.dtable_uuid)),
            'file_type': file_type,
            'relative_path': parse.quote(relative_path.strip('/')),
            'filename': parse.quote(d.get('name', name))
        }
        return {
            'type': file_type,
            'size': d.get('size'),
            'name': d.get('name'),
            'url': url
        }
