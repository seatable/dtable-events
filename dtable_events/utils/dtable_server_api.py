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


class WrongFilterException(Exception):
    pass


class NotFoundException(Exception):
    pass


def parse_response(response):
    if response.status_code >= 400:
        if response.status_code == 404:
            raise NotFoundException()
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

    def __init__(self, username, dtable_uuid, dtable_server_url, server_url=None, repo_id=None, workspace_id=None, timeout=180):
        self.username = username
        self.dtable_uuid = dtable_uuid
        self.headers = None
        self.dtable_server_url = dtable_server_url.rstrip('/')
        self.server_url = server_url.rstrip('/') if server_url else None
        self.repo_id = repo_id
        self.workspace_id = workspace_id
        self.timeout = timeout
        self._init()

    def _init(self):
        dtable_server_access_token = get_dtable_server_token(self.username, self.dtable_uuid)
        self.headers = {'Authorization': 'Token ' + dtable_server_access_token}

    def get_metadata(self):
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/?from=dtable_events'
        response = requests.get(url, headers=self.headers, timeout=self.timeout)
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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def list_rows(self, table_name, start=None, limit=None):
        logger.debug('list rows table_name: %s', table_name)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        params = {
            'table_name': table_name,
        }
        if start is not None and limit is not None:
            params['start'] = start
            params['limit'] = limit
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('rows')

    def get_row(self, table_name, row_id, convert_link_id=False):
        """
        :param table_name: str
        :param row_id: str
        :return: dict
        """
        logger.debug('get row table_name: %s row_id: %s', table_name, row_id)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/' + row_id + '/?from=dtable_events'
        params = {
            'table_name': table_name,
            'convert_link_id': convert_link_id
        }
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data

    def list_columns(self, table_name, view_name=None):
        logger.debug('list columns table_name: %s view_name: %s', table_name, view_name)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/columns/?from=dtable_events'
        params = {'table_name': table_name}
        if view_name:
            params['view_name'] = view_name
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('columns')

    def view_rows(self, table_name, view_name, has_hidden_columns):
        url = self.dtable_server_url + '/api/v1/internal/dtables/' + self.dtable_uuid + '/view-rows/?from=dtable_events'
        params = {
            'table_name': table_name,
            'view_name': view_name,
            'convert_link_id': True,
            'has_hidden_columns': has_hidden_columns,
        }
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data.get('rows')

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
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        data = parse_response(response)
        return data

    def batch_append_rows(self, table_name, rows_data):
        logger.debug('batch append rows table_name: %s rows_data: %s', table_name, rows_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-append-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'rows': rows_data,
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def append_row(self, table_name, row_data):
        logger.debug('append row table_name: %s row_data: %s', table_name, row_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row': row_data
        }
        response = requests.post(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def update_row(self, table_name, row_id, row_data):
        logger.debug('update row table_name: %s row_id: %s row_data: %s', table_name, row_id, row_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_id': row_id,
            'row': row_data
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def batch_update_rows(self, table_name, rows_data):
        logger.debug('batch update rows table_name: %s rows_data: %s', table_name, rows_data)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-update-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'updates': rows_data,
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def add_column_options(self, table_name, column_name, options):
        logger.debug('add column options, table_name: %s , column name: %s, options: %s', table_name, column_name, options)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/column-options/?from=dtable_events'

        data = {
            'table_name': table_name,
            'column': column_name,
            'options': options
        }

        response = requests.post(url, json=data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def batch_delete_rows(self, table_name, row_ids):
        logger.debug('batch delete rows table_name: %s row_ids: %s', table_name, row_ids)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/batch-delete-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids,
        }
        response = requests.delete(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def lock_rows(self, table_name, row_ids):
        logger.debug('lock rows table_name: %s row_ids: %s', table_name, row_ids)
        url = self.dtable_server_url + '/api/v1/dtables/' + self.dtable_uuid + '/lock-rows/?from=dtable_events'
        json_data = {
            'table_name': table_name,
            'row_ids': row_ids
        }
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
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
        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

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

        response = requests.put(url, json=json_data, headers=self.headers, timeout=self.timeout)
        return parse_response(response)

    def get_file_upload_link(self, attach_path=None):
        """
        :return: dict
        """
        repo_id = self.repo_id
        asset_dir_path = '/asset/' + self.dtable_uuid
        if attach_path:
            asset_dir_path = os.path.join('/asset', self.dtable_uuid, attach_path)
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

    def upload_email_attachment(self, name, content: bytes, email_id):
        file_type = 'file'
        attach_path = os.path.join('emails', str(datetime.today())[:7], email_id)
        upload_link_dict = self.get_file_upload_link(attach_path)
        parent_dir = upload_link_dict['parent_path']
        upload_link = upload_link_dict['upload_link'] + '?ret-json=1'

        response = requests.post(upload_link, data={
            'parent_dir': parent_dir,
            'replace': 0,
        }, files={
            'file': (name, io.BytesIO(content))
        }, timeout=120)

        d = response.json()[0]
        url = '%(server)s/workspace/%(workspace_id)s/%(parent_dir)s/%(filename)s' % {
            'server': self.server_url.strip('/'),
            'workspace_id': self.workspace_id,
            'parent_dir': parent_dir.strip('/'),
            'file_type': file_type,
            'filename': parse.quote(d.get('name', name))
        }

        return {
            'type': file_type,
            'size': d.get('size'),
            'name': d.get('name'),
            'url': url
        }
