import json
import requests
import os
import time
import logging
import io
import uuid
import multiprocessing
import datetime
import random
import string
import jwt
import sys
from io import BytesIO
from zipfile import ZipFile, is_zipfile

from django.utils.http import urlquote
from seaserv import seafile_api

from dtable_events.dtable_io.task_manager import task_manager

# this two prefix used in exported zip file
FILE_URL_PREFIX = 'file://dtable-bundle/asset/files/'
IMG_URL_PREFIX = 'file://dtable-bundle/asset/images/'
EXCEL_DIR_PATH = '/tmp/excel/'


def setup_logger(logname):
    """
    setup logger for dtable io
    """
    logdir = os.path.join(os.environ.get('LOG_DIR', ''))
    log_file = os.path.join(logdir, logname)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)

    logger = multiprocessing.get_logger()
    logger.addHandler(handler)

    return logger


def gen_inner_file_get_url(token, filename):
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/files/%s/%s' % (INNER_FILE_SERVER_ROOT, token,
                               urlquote(filename))


def gen_inner_file_upload_url(token, op, replace=False):
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    url = '%s/%s/%s' % (INNER_FILE_SERVER_ROOT, op, token)
    if replace is True:
        url += '?replace=1'
    return url


def get_dtable_server_token(username, dtable_uuid):
    DTABLE_PRIVATE_KEY = str(task_manager.conf['dtable_private_key'])
    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': username,
        'permission': 'rw',
    }
    access_token = jwt.encode(
        payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
    )

    return access_token


def gen_dir_zip_download_url(token):
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/zip/%s' % (INNER_FILE_SERVER_ROOT, token)


def convert_dtable_export_file_and_image_url(dtable_content):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded
    """

    tables = dtable_content.get('tables', [])
    for table in tables:
        rows = table.get('rows', [])
        long_text_cols = {col['key'] for col in table.get('columns', []) if col['type'] == 'long-text'}
        for row in rows:
            for k, v in row.items():
                if isinstance(v, list):
                    for idx, item in enumerate(v):
                        # case1: image
                        if isinstance(item, str) and 'http' in item and 'images' in item:
                            img_name = '/'.join(item.split('/')[-2:])  # e.g. "2020-01/WeWork%20gg.png"
                            v[idx] = IMG_URL_PREFIX + img_name
                        # case2: file
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k == 'url':
                                    file_name = '/'.join(v.split('/')[-2:]) # e.g. 2020-01/README.md
                                    item[k] = FILE_URL_PREFIX + file_name
                # long-text with images
                if k in long_text_cols and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        if isinstance(item, str) and 'http' in item and 'images' in item:
                            img_name = '/'.join(item.split('/')[-2:])
                            v['images'][idx] = IMG_URL_PREFIX + img_name
                            v['text'] = v['text'].replace(item, v['images'][idx])
    return dtable_content


def prepare_dtable_json(repo_id, dtable_uuid, table_name, dtable_file_dir_id):
    """
    used in export dtable
    create dtable json file at /tmp/dtable-io/<dtable_uuid>/dtable_asset/content.json,
    so that we can zip /tmp/dtable-io/<dtable_uuid>/dtable_asset

    :param repo_id:            repo of this dtable
    :param table_name:         name of dtable
    :param dtable_file_dir_id: xxx.dtable's file dir id
    :return:                   file stream
    """
    try:
        token = seafile_api.get_fileserver_access_token(
            repo_id, dtable_file_dir_id, 'download', '', use_onetime=False
        )
    except Exception as e:
        raise e

    json_url = gen_inner_file_get_url(token, table_name + '.dtable')
    content_json = requests.get(json_url).content
    if content_json:
        dtable_content = convert_dtable_export_file_and_image_url(json.loads(content_json))
    else:
        dtable_content = ''
    content_json = json.dumps(dtable_content).encode('utf-8')
    path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_asset', 'content.json')

    with open(path, 'wb') as f:
       f.write(content_json)

def prepare_dtable_json_from_memory(dtable_uuid, username):
    """
    Used in dtable file export in real-time from memory by request the api of dtable-server
    It is more effective than exporting dtable files from seafile-server which will take about 5 minutes
    for synchronizing the data from memory to seafile-server.
    :param dtable_uuid:
    :param username:
    :return:
    """
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    json_url = api_url.rstrip('/') + '/dtables/' + dtable_uuid + '/'
    content_json = requests.get(json_url, headers=headers).content
    if content_json:
        dtable_content = convert_dtable_export_file_and_image_url(json.loads(content_json))
    else:
        dtable_content = ''
    content_json = json.dumps(dtable_content).encode('utf-8')
    path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_asset', 'content.json')

    with open(path, 'wb') as f:
        f.write(content_json)


def prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id):
    """
    used in export dtable
    create asset folder at /tmp/dtable-io/<dtable_uuid>/dtable_asset
    notice that create_dtable_json and this function create file at same directory

    1. get asset zip from file_server
    2. unzip it at /tmp/dtable-io/<dtable_uuid>/dtable_asset/

    :param username:
    :param repo_id:
    :param asset_dir_id:
    :return:
    """

    # get file server access token
    fake_obj_id = {
        'obj_id': asset_dir_id,
        'dir_name': 'asset',        # after download and zip, folder root name is asset
        'is_windows': 0
    }
    try:
        token = seafile_api.get_fileserver_access_token(
            repo_id, json.dumps(fake_obj_id), 'download-dir', username, use_onetime=False
    )
    except Exception as e:
        raise e

    progress = {'zipped': 0, 'total': 1}
    while progress['zipped'] != progress['total']:
        time.sleep(0.5)   # sleep 0.5 second
        try:
            progress = json.loads(seafile_api.query_zip_progress(token))
        except Exception:
            raise Exception

    asset_url = gen_dir_zip_download_url(token)
    resp = requests.get(asset_url)
    file_obj = io.BytesIO(resp.content)
    if is_zipfile(file_obj):
        with ZipFile(file_obj) as zp:
            zp.extractall(os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_asset'))


def copy_src_forms_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = "SELECT `username`, `form_config`, `share_type` FROM dtable_forms WHERE dtable_uuid=:dtable_uuid"
    src_forms = db_session.execute(sql, {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_forms_json = []
    for src_form in src_forms:
        form = {
            'username': src_form[0],
            'form_config': src_form[1],
            'share_type': src_form[2],
        }
        src_forms_json.append(form)
    if src_forms_json:
        # os.makedirs(os.path.join(tmp_file_path, 'forms.json'))
        with open(os.path.join(tmp_file_path, 'forms.json'), 'w+') as fp:
            fp.write(json.dumps(src_forms_json))


def convert_dtable_import_file_and_image_url(dtable_content, workspace_id, dtable_uuid):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded

    :param dtable_content: python dict
    :param workspace_id:
    :param dtable_uuid:
    :return:  python dict
    """
    tables = dtable_content.get('tables', [])

    # handle different url in settings.py
    dtable_web_service_url = task_manager.conf['dtable_web_service_url'].rstrip('/')

    for table in tables:
        rows = table.get('rows', [])
        long_text_cols = {col['key'] for col in table.get('columns', []) if col['type'] == 'long-text'}
        for idx, row in enumerate(rows):
            for k, v in row.items():
                if isinstance(v, list):
                    for idx, item in enumerate(v):
                        # case1: image
                        if isinstance(item, str) and item.startswith(IMG_URL_PREFIX):
                            img_name = '/'.join(item.split('/')[-2:])  # e.g. "2020-01/WeWork%20gg.png"
                            new_url = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                               dtable_uuid, 'images', img_name])
                            v[idx] = new_url
                        # case2: file
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k == 'url' and v.startswith(FILE_URL_PREFIX):
                                    file_name = '/'.join(v.split('/')[-2:]) # e.g. 2020-01/README.md
                                    new_url = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                                       dtable_uuid, 'files', file_name])

                                    item[k] = new_url
                # long-text with images
                if k in long_text_cols and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        if isinstance(item, str) and item.startswith(IMG_URL_PREFIX):
                            img_name = '/'.join(item.split('/')[-2:])
                            new_url = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                               dtable_uuid, 'images', img_name])
                            v['images'][idx] = new_url
                            v['text'] = v['text'].replace(item, v['images'][idx])
    return dtable_content


def post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name):
    """
    used to import dtable
    prepare dtable json file and post it at file server

    :param repo_id:
    :param workspace_id:
    :param dtable_uuid:         str
    :param dtable_file_name:    xxx.dtable, the name of zip we imported
    :return:
    """
    # change url in content json, then save it at file server
    content_json_file_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'content.json')
    with open(content_json_file_path, 'r') as f:
        content_json = f.read()

    try:
        content = json.loads(content_json)
    except:
        content = ''
    if not content:
        seafile_api.post_empty_file(repo_id, '/', dtable_file_name, username)
        return

    content_json = convert_dtable_import_file_and_image_url(content, workspace_id, dtable_uuid)
    with open(content_json_file_path, 'w') as f:
        f.write(json.dumps(content_json))

    try:
        seafile_api.post_file(repo_id, content_json_file_path, '/', dtable_file_name, username)
    except Exception as e:
        raise e


def post_asset_files(repo_id, dtable_uuid, username):
    """
    used to import dtable
    post asset files in  /tmp/dtable-io/<dtable_uuid>/dtable_zip_extracted/ to file server

    :return:
    """
    asset_root_path = os.path.join('/asset', dtable_uuid)

    tmp_extracted_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/')
    for root, dirs, files in os.walk(tmp_extracted_path):
        for file_name in files:
            if file_name in ['content.json', 'forms.json']:
                continue
            inner_path = root[len(tmp_extracted_path)+6:]  # path inside zip
            tmp_file_path = os.path.join(root, file_name)
            cur_file_parent_path = os.path.join(asset_root_path, inner_path)
            # check current file's parent path before post file
            path_id = seafile_api.get_dir_id_by_path(repo_id, cur_file_parent_path)
            if not path_id:
                seafile_api.mkdir_with_parents(repo_id, '/', cur_file_parent_path[1:], username)

            seafile_api.post_file(repo_id, tmp_file_path, cur_file_parent_path, file_name, username)


def gen_form_id(length=4):
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(length))


def add_a_form_to_db(form, workspace_id, dtable_uuid, db_session):
    # check form id
    form_id = gen_form_id()
    sql_check_form_id = 'SELECT `id` FROM dtable_forms WHERE form_id=:form_id'
    while db_session.execute(sql_check_form_id, {'form_id': form_id}).rowcount > 0:
        form_id = gen_form_id()

    sql = "INSERT INTO dtable_forms (`username`, `workspace_id`, `dtable_uuid`, `form_id`, `form_config`, `token`, `share_type`, `created_at`)" \
        "VALUES (:username, :workspace_id, :dtable_uuid, :form_id, :form_config, :token, :share_type, :created_at)"

    db_session.execute(sql, {
        'username': form['username'],
        'workspace_id': workspace_id,
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'form_id': form_id,
        'form_config': form['form_config'],
        'token': str(uuid.uuid4()),
        'share_type': form['share_type'],
        'created_at': datetime.datetime.now(),
        })
    db_session.commit()


def create_forms_from_src_dtable(workspace_id, dtable_uuid, db_session):
    if not db_session:
        return
    forms_json_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'forms.json')
    if not os.path.exists(forms_json_path):
        return
    
    with open(forms_json_path, 'r') as fp:
        forms_json = fp.read()
    forms = json.loads(forms_json)
    for form in forms:
        if ('username' not in form) or ('form_config' not in form) or ('share_type' not in form):
            continue
        add_a_form_to_db(form, workspace_id, dtable_uuid, db_session)


def download_files_to_path(username, repo_id, dtable_uuid, files, path, files_map=None):
    """
    download dtable's asset files to path
    """
    valid_file_obj_ids = []
    base_path = os.path.join('/asset', dtable_uuid)
    for file in files:
        full_path = os.path.join(base_path, *file.split('/'))
        obj_id = seafile_api.get_file_id_by_path(repo_id, full_path)
        if not obj_id:
            continue
        valid_file_obj_ids.append((file, obj_id))

    tmp_file_list = []
    for file, obj_id in valid_file_obj_ids:
        token = seafile_api.get_fileserver_access_token(
            repo_id, obj_id, 'download', username,
            use_onetime=False
        )
        file_name = os.path.basename(file)
        if files_map and files_map.get(file, None):
            file_name = files_map.get(file)
        file_url = gen_inner_file_get_url(token, file_name)
        content = requests.get(file_url).content
        filename_by_path = os.path.join(path, file_name)
        with open(filename_by_path, 'wb') as f:
            f.write(content)
        tmp_file_list.append(filename_by_path)
    return tmp_file_list

def get_excel_file(repo_id, file_name):
    from dtable_events.dtable_io import dtable_io_logger

    file_path = EXCEL_DIR_PATH + file_name + '.xlsx'
    obj_id = seafile_api.get_file_id_by_path(repo_id, file_path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, obj_id, 'download', '', use_onetime=True
    )
    url = gen_inner_file_get_url(token, file_name + '.xlsx')
    content = requests.get(url).content

    file_size = sys.getsizeof(content)
    dtable_io_logger.info('excel file size: %d KB' % (file_size >> 10))
    return BytesIO(content)

def get_csv_file(repo_id, file_name):
    from dtable_events.dtable_io import dtable_io_logger

    file_path = EXCEL_DIR_PATH + file_name + '.csv'
    obj_id = seafile_api.get_file_id_by_path(repo_id, file_path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, obj_id, 'download', '', use_onetime=True
    )
    url = gen_inner_file_get_url(token, file_name + '.csv')
    content = requests.get(url).content

    file_size = sys.getsizeof(content)
    dtable_io_logger.info('csv file size: %d KB' % (file_size >> 10))
    return BytesIO(content)

def upload_excel_json_file(repo_id, file_name, content):
    from dtable_events.dtable_io import dtable_io_logger

    obj_id = json.dumps({'parent_dir': EXCEL_DIR_PATH})
    token = seafile_api.get_fileserver_access_token(
        repo_id, obj_id, 'upload', '', use_onetime=True
    )
    upload_link = gen_inner_file_upload_url(token, 'upload-api', replace=True)
    content_type = 'application/json'

    file = content.encode('utf-8')
    file_size = sys.getsizeof(file)
    dtable_io_logger.info( 'excel json file size: %d KB' % (file_size >> 10))
    response = requests.post(upload_link, 
        data = {'parent_dir': EXCEL_DIR_PATH, 'relative_path': '', 'replace': 1},
        files = {'file': (file_name + '.json', file, content_type)}
    )

def get_excel_json_file(repo_id, file_name):
    file_path = EXCEL_DIR_PATH + file_name + '.json'
    file_id = seafile_api.get_file_id_by_path(repo_id, file_path)
    if not file_id:
        raise FileExistsError('file %s not found' % file_path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, file_id, 'download', '', use_onetime=True
    )
    url = gen_inner_file_get_url(token, file_name + '.json')
    json_file = requests.get(url).content
    return json_file

def delete_excel_file(username, repo_id, file_name):
    filename = file_name + '.xlsx\t' + file_name + '.json\t'
    seafile_api.del_file(repo_id, EXCEL_DIR_PATH, filename, username)

def upload_excel_json_to_dtable_server(username, dtable_uuid, json_file):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/import-excel/'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}

    files = {
        'excel_json': json_file
    }
    res = requests.post(url, headers=headers, files=files)
    if res.status_code != 200:
        raise ConnectionError('failed to import excel json %s %s' % (dtable_uuid, res.text))

def append_excel_json_to_dtable_server(username, dtable_uuid, json_file, table_name):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-append-rows/'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    rows_data = json.loads(json_file.decode())[0]['rows']
    offset = 0
    while True:
        rows = rows_data[offset: offset + 1000]
        offset = offset + 1000
        if not rows:
            break
        json_data = {
            'table_name': table_name,
            'rows': rows,
        }
        res = requests.post(url, headers=headers, json=json_data)
        if res.status_code != 200:
            raise ConnectionError('failed to append excel json %s %s' % (dtable_uuid, res.text))
        time.sleep(0.5)


def get_columns_from_dtable_server(username, dtable_uuid, table_name):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/columns/?' + 'table_name=' + table_name
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise ConnectionError('failed to get columns %s %s' % (dtable_uuid, res.text))
    return json.loads(res.content.decode()).get('columns', [])


def get_rows_from_dtable_server(username, dtable_uuid, table_name):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/?' + 'table_name=' + table_name
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise ConnectionError('failed to get rows %s %s' % (dtable_uuid, res.text))
    return json.loads(res.content.decode()).get('rows', [])

def update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-update-rows/'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    offset = 0
    while True:
        rows = update_rows[offset: offset + 1000]
        offset = offset + 1000
        if not rows:
            break
        json_data = {
            'table_name': table_name,
            'updates': rows,
        }
        res = requests.put(url, headers=headers, json=json_data)
        if res.status_code != 200:
            raise ConnectionError('failed to update excel json %s %s' % (dtable_uuid, res.text))
        time.sleep(0.5)

def update_append_excel_json_to_dtable_server(username, dtable_uuid, rows_data, table_name):
    DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
    ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['enable_dtable_server_cluster']
    DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-append-rows/'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    offset = 0
    while True:
        rows = rows_data[offset: offset + 1000]
        offset = offset + 1000
        if not rows:
            break
        json_data = {
            'table_name': table_name,
            'rows': rows,
        }
        res = requests.post(url, headers=headers, json=json_data)
        if res.status_code != 200:
            raise ConnectionError('failed to append excel json %s %s' % (dtable_uuid, res.text))
        time.sleep(0.5)

def delete_file(username, repo_id, file_name):
    filename = file_name + '.xlsx\t' + file_name + '.json\t' + file_name + '.csv\t'
    m = seafile_api.del_file(repo_id, EXCEL_DIR_PATH, filename, username)

def guess_CSV_delimiter(text):
    import re
    lines = text.split('\n')
    comma_count = len(re.compile(r',').findall(lines[0]))
    semicolon_count = len(re.compile(r';').findall(lines[0]))
    return ';' if comma_count < semicolon_count else ','
