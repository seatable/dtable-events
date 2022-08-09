import json

import requests
import os
import time
import logging
from logging import handlers
import io
import uuid
import datetime
import random
import string
import jwt
import sys
import re
from io import BytesIO
from zipfile import ZipFile, is_zipfile
from dateutil import parser
from uuid import UUID

from django.utils.http import urlquote
from seaserv import seafile_api

from dtable_events.dtable_io.external_app import APP_USERS_COUMNS_TYPE_MAP, match_user_info, update_app_sync, \
    get_row_ids_for_delete, get_app_users
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.utils import get_inner_dtable_server_url

# this two prefix used in exported zip file
from dtable_events.utils.constants import ColumnTypes


FILE_URL_PREFIX = 'file://dtable-bundle/asset/files/'
IMG_URL_PREFIX = 'file://dtable-bundle/asset/images/'
EXCEL_DIR_PATH = '/tmp/excel/'


def setup_logger(logname):
    """
    setup logger for dtable io
    """
    logdir = os.path.join(os.environ.get('LOG_DIR', ''))
    log_file = os.path.join(logdir, logname)
    handler = handlers.TimedRotatingFileHandler(log_file, when='MIDNIGHT', interval=1, backupCount=7)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    handler.addFilter(logging.Filter(logname))

    logger = logging.getLogger(logname)
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


def convert_dtable_export_file_and_image_url(workspace_id, dtable_uuid, dtable_content):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded
    """
    from dtable_events.dtable_io import dtable_io_logger

    tables = dtable_content.get('tables', [])
    old_file_part_path = '/workspace/%s/asset/%s/' % (workspace_id, str(UUID(dtable_uuid)))
    dtable_io_logger.debug('old_file_part_path: %s', old_file_part_path)
    for table in tables:
        rows = table.get('rows', [])
        dtable_io_logger.debug('table: %s rows: %s', table['_id'], len(rows))
        cols_dict = {col['key']: col for col in table.get('columns', [])}
        for row in rows:
            for k, v in row.items():
                if k not in cols_dict:
                    continue
                col = cols_dict[k]
                if col['type'] == ColumnTypes.IMAGE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, str) and old_file_part_path in item:
                            img_name = '/'.join(item.split('/')[-2:])  # e.g. "2020-01/WeWork%20gg.png"
                            v[idx] = IMG_URL_PREFIX + img_name
                elif col['type'] == ColumnTypes.FILE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, dict) and old_file_part_path in item.get('url', ''):
                            file_name = '/'.join(item['url'].split('/')[-2:])
                            item['url'] = FILE_URL_PREFIX + file_name
                elif col['type'] == ColumnTypes.LONG_TEXT and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        if old_file_part_path in item:
                            img_name = '/'.join(item.split('/')[-2:])
                            v['images'][idx] = IMG_URL_PREFIX + img_name
                            v['text'] = v['text'].replace(item, v['images'][idx])
    return dtable_content


def prepare_dtable_json_from_memory(workspace_id, dtable_uuid, username):
    """
    Used in dtable file export in real-time from memory by request the api of dtable-server
    It is more effective than exporting dtable files from seafile-server which will take about 5 minutes
    for synchronizing the data from memory to seafile-server.
    :param dtable_uuid:
    :param username:
    :return:
    """
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
    api_url = get_inner_dtable_server_url()
    json_url = api_url.rstrip('/') + '/dtables/' + dtable_uuid + '/?from=dtable_events'
    content_json = requests.get(json_url, headers=headers).content
    if content_json:
        try:
            json_content = json.loads(content_json)
        except Exception as e:
            raise Exception('decode json error: %s' % content_json.decode()[0:200])
        dtable_content = convert_dtable_export_file_and_image_url(workspace_id, dtable_uuid, json_content)
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
        except Exception as e:
            raise e

    asset_url = gen_dir_zip_download_url(token)
    try:
        resp = requests.get(asset_url)
    except Exception as e:
        raise e
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


def copy_src_auto_rules_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = """SELECT `run_condition`, `trigger`, `actions` FROM dtable_automation_rules WHERE dtable_uuid=:dtable_uuid"""
    src_auto_rules = db_session.execute(sql, {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_auto_rules_json = []
    for src_auto_rule in src_auto_rules:
        auto_rule = {
            'run_condition': src_auto_rule[0],
            'trigger': src_auto_rule[1],
            'actions': src_auto_rule[2],
        }
        src_auto_rules_json.append(auto_rule)
    if src_auto_rules_json:
        with open(os.path.join(tmp_file_path, 'auto_rules.json'), 'w+') as fp:
            fp.write(json.dumps(src_auto_rules_json))


def copy_src_workflows_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = """SELECT `workflow_config` FROM dtable_workflows WHERE dtable_uuid=:dtable_uuid"""
    src_workflows = db_session.execute(sql, {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_workflows_json = []
    for src_workflow in src_workflows:
        workflow = {
            'workflow_config': src_workflow[0]
        }
        src_workflows_json.append(workflow)
    if src_workflows_json:
        with open(os.path.join(tmp_file_path, 'workflows.json'), 'w+') as fp:
            fp.write(json.dumps(src_workflows_json))


def copy_src_external_app_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = """SELECT `app_config` FROM dtable_external_apps WHERE dtable_uuid=:dtable_uuid"""
    src_external_apps = db_session.execute(sql, {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_external_apps_json = []
    for src_external_app in src_external_apps:
        external_app = {
            'app_config': json.loads(src_external_app[0])
        }
        src_external_apps_json.append(external_app)
    if src_external_apps_json:
        with open(os.path.join(tmp_file_path, 'external_apps.json'), 'w+') as fp:
            fp.write(json.dumps(src_external_apps_json))


def convert_dtable_import_file_url(dtable_content, workspace_id, dtable_uuid):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded

    :param dtable_content: python dict
    :param workspace_id:
    :param dtable_uuid:
    :return:  python dict
    """
    from dtable_events.dtable_io import dtable_io_logger

    tables = dtable_content.get('tables', [])

    # handle different url in settings.py
    dtable_web_service_url = task_manager.conf['dtable_web_service_url'].rstrip('/')

    for table in tables:
        rows = table.get('rows', [])
        dtable_io_logger.debug('table: %s rows: %s', table['_id'], len(rows))
        cols_dict = {col['key']: col for col in table.get('columns', [])}
        for idx, row in enumerate(rows):
            for k, v in row.items():
                if k not in cols_dict:
                    continue
                col = cols_dict[k]
                if col['type'] == ColumnTypes.IMAGE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, str) and IMG_URL_PREFIX in item:
                            img_name = '/'.join(item.split('/')[-2:])
                            new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                str(UUID(dtable_uuid)), 'images', img_name])
                            v[idx] = new_url
                elif col['type'] == ColumnTypes.FILE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, dict) and FILE_URL_PREFIX in item.get('url', ''):
                            file_name = '/'.join(item['url'].split('/')[-2:])
                            new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                str(UUID(dtable_uuid)), 'files', file_name])
                            item['url'] = new_url
                elif col['type'] == ColumnTypes.LONG_TEXT and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        if IMG_URL_PREFIX in item:
                            img_name = '/'.join(item.split('/')[-2:])
                            new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                str(UUID(dtable_uuid)), 'images', img_name])
                            v['images'][idx] = new_url
                            v['text'] = v['text'].replace(item, v['images'][idx])
    
    plugin_settings = dtable_content.get('plugin_settings', {})

    # page desgin settings
    page_design_settings = plugin_settings.get('page-design', [])
    for page in page_design_settings:
        page_id = page['page_id'];
        page['content_url'] = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                        dtable_uuid, 'page-design', page_id, '%s.json'%(page_id)])
        page['poster_url'] = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                        dtable_uuid, 'page-design', page_id, '%s.png'%(page_id)])
    
    return dtable_content


def post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage):
    """
    used to import dtable
    prepare dtable json file and post it at file server

    :param repo_id:
    :param workspace_id:
    :param dtable_uuid:         str
    :param dtable_file_name:    xxx.dtable, the name of zip we imported
    :return:
    """
    from dtable_events.utils.storage_backend import storage_backend

    # change url in content json, then save it at file server
    content_json_file_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'content.json')
    with open(content_json_file_path, 'r') as f:
        content_json = f.read()

    try:
        content = json.loads(content_json)
    except:
        content = ''
    if not content:
        try:
            storage_backend.create_empty_dtable(dtable_uuid, username, in_storage, repo_id, dtable_file_name)
        except Exception as e:
            raise e
        return

    content_json = convert_dtable_import_file_url(content, workspace_id, dtable_uuid)

    try:
        storage_backend.save_dtable(dtable_uuid, json.dumps(content_json), username, in_storage, repo_id, dtable_file_name)
    except Exception as e:
        raise e
    
    return content_json


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

# execute after post asset
# page_design_settings, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, username
def update_page_design_static_image(page_design_settings, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, dtable_web_service_url, file_server_port, username):
    if not isinstance(page_design_settings, list):
        return
    
    valid_dtable_web_service_url = dtable_web_service_url.strip('/')
    inner_file_server_root = 'http://127.0.0.1:' + str(file_server_port)
    for page in page_design_settings:
        page_id = page['page_id']
        page_content_file_name = '%s.json'%(page_id)
        page_content_url = page['content_url']
        parent_dir = '/asset/%s/page-design/%s'%(dtable_uuid, page_id)
        page_json_file_id = seafile_api.get_file_id_by_path(repo_id, '/asset' + page_content_url.split('asset')[1])
        token = seafile_api.get_fileserver_access_token(
            repo_id, page_json_file_id, 'view', '', use_onetime=False
        )
        content_url = '%s/files/%s/%s'%(inner_file_server_root, token,
                                urlquote(page_content_file_name))
        page_content_response = requests.get(content_url)
        is_changed = False
        if page_content_response.status_code == 200:
            page_content = page_content_response.json()
            if 'pages' not in page_content.keys():
                page_elements = page_content.get('page_elements', {})
                page_content = {
                    'page_id': page_id,
                    'default_font': page_content.get('default_font', ''),
                    'page_settings': page_content.get('page_settings', {}),
                    'pages': [
                        {
                            '_id': '0000',
                            'element_map': page_elements.get('element_map', {}),
                            'element_ids': page_elements.get('element_ids', [])
                        }
                    ]
                }
            pages = page_content.get('pages', [])
            for sub_page in pages:
                element_map = sub_page.get('element_map', {})
                for element_id in element_map:
                    element = element_map.get(element_id, {})
                    if element['type'] == 'static_image':
                        config_data = element.get('config_data', {})
                        static_image_url = config_data.get('staticImageUrl', '')
                        file_name = '/'.join(static_image_url.split('/')[-2:])
                        config_data['staticImageUrl'] = '/'.join([valid_dtable_web_service_url, 'workspace', str(workspace_id),
                                                                'asset', str(dtable_uuid), 'page-design', page_id, file_name])
                        is_changed = True
            if is_changed:
                if not os.path.exists(content_json_tmp_path):
                    os.makedirs(content_json_tmp_path)
                page_content_save_path = os.path.join(content_json_tmp_path, page_content_file_name)
                with open(page_content_save_path, 'w') as f:
                    json.dump(page_content, f)
                seafile_api.put_file(repo_id, page_content_save_path, parent_dir, '%s.json'%(page_id), username, None)


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


def add_a_auto_rule_to_db(username, auto_rule, workspace_id, dtable_uuid, db_session):
    # get org_id
    sql_get_org_id = """SELECT `org_id` FROM workspaces WHERE id=:id"""
    org_id = [x[0] for x in db_session.execute(sql_get_org_id, {'id': workspace_id})][0]

    sql = """INSERT INTO dtable_automation_rules (`dtable_uuid`, `run_condition`, `trigger`, `actions`,
             `creator`, `ctime`, `org_id`, `last_trigger_time`) VALUES (:dtable_uuid, :run_condition,
             :trigger, :actions, :creator, :ctime, :org_id, :last_trigger_time)"""
    db_session.execute(sql, {
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'run_condition': auto_rule['run_condition'],
        'trigger': auto_rule['trigger'],
        'actions': auto_rule['actions'],
        'creator': username,
        'ctime': datetime.datetime.utcnow(),
        'org_id': org_id,
        'last_trigger_time': None,
        })
    db_session.commit()


def add_a_workflow_to_db(username, workflow, workspace_id, dtable_uuid, owner, db_session):
    sql = """INSERT INTO dtable_workflows (`token`,`dtable_uuid`, `workflow_config`, `creator`, `created_at`,
             `owner`) VALUES (:token, :dtable_uuid, :workflow_config,
             :creator, :created_at, :owner)"""
    db_session.execute(sql, {
        'token': str(uuid.uuid4()),
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'workflow_config': workflow['workflow_config'],
        'creator': username,
        'created_at': datetime.datetime.utcnow(),
        'owner': owner,
        })
    db_session.commit()


def add_an_external_app_to_db(username, external_app, dtable_uuid, db_session, org_id):
    sql = """INSERT INTO dtable_external_apps (`token`,`dtable_uuid`,`app_type`, `app_config`, `creator`, `created_at`, `org_id`) 
                VALUES (:token, :dtable_uuid, :app_type, :app_config, :creator, :created_at, :org_id)"""

    db_session.execute(sql, {
        'token': str(uuid.uuid4()),
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'app_type': external_app['app_config'].get('app_type'),
        'app_config': json.dumps(external_app['app_config']),
        'creator': username,
        'created_at': datetime.datetime.utcnow(),
        'org_id': org_id
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


def create_auto_rules_from_src_dtable(username, workspace_id, dtable_uuid, db_session):
    if not db_session:
        return
    auto_rules_json_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'auto_rules.json')
    if not os.path.exists(auto_rules_json_path):
        return
    with open(auto_rules_json_path, 'r') as fp:
        auto_rules_json = fp.read()
    auto_rules = json.loads(auto_rules_json)
    for auto_rule in auto_rules:
        if ('run_condition' not in auto_rule) or ('trigger' not in auto_rule) or ('actions' not in auto_rule):
            continue
        add_a_auto_rule_to_db(username, auto_rule, workspace_id, dtable_uuid, db_session)


def create_workflows_from_src_dtable(username, workspace_id, dtable_uuid, owner, db_session):
    if not db_session:
        return
    workflows_json_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'workflows.json')
    if not os.path.exists(workflows_json_path):
        return
    with open(workflows_json_path, 'r') as fp:
        workflows_json = fp.read()
    workflows = json.loads(workflows_json)
    for workflow in workflows:
        if 'workflow_config' not in workflow:
            continue
        add_a_workflow_to_db(username, workflow, workspace_id, dtable_uuid, owner, db_session)


def create_external_apps_from_src_dtable(username, dtable_uuid, db_session, org_id):
    if not db_session:
        return
    external_apps_json_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'external_apps.json')
    if not os.path.exists(external_apps_json_path):
        return
    with open(external_apps_json_path, 'r') as fp:
        external_apps_json = fp.read()
    external_apps = json.loads(external_apps_json)

    for external_app in external_apps:
        if 'app_config' not in external_app:
            continue
        add_an_external_app_to_db(username, external_app, dtable_uuid, db_session, org_id)


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
    filenames = [file_name + '.xlsx', file_name + '.json']
    seafile_api.del_file(repo_id, EXCEL_DIR_PATH, json.dumps(filenames), username)

def upload_excel_json_to_dtable_server(username, dtable_uuid, json_file, lang='en'):
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/import-excel/?from=dtable_events&lang=' + lang
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}

    files = {
        'excel_json': json_file
    }

    res = requests.post(url, headers=headers, files=files)
    if res.status_code != 200:
        raise ConnectionError('failed to import excel json %s %s' % (dtable_uuid, res.text))

def upload_excel_json_add_table_to_dtable_server(username, dtable_uuid, json_file, lang='en'):
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/import-excel-add-table/?from=dtable_events&lang=' + lang
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}

    files = {
        'excel_json': json_file
    }
    res = requests.post(url, headers=headers, files=files)
    if res.status_code != 200:
        raise ConnectionError('failed to import excel json %s %s' % (dtable_uuid, res.text))

def append_excel_json_to_dtable_server(username, dtable_uuid, json_file, table_name):
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-append-rows/?from=dtable_events'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
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
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/columns/?from=dtable_events&table_name=' + table_name
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise ConnectionError('failed to get columns %s %s' % (dtable_uuid, res.text))
    return json.loads(res.content.decode()).get('columns', [])


def get_csv_file(repo_id, file_name):
    from dtable_events.dtable_io import dtable_io_logger

    file_path = EXCEL_DIR_PATH + file_name + '.csv'
    obj_id = seafile_api.get_file_id_by_path(repo_id, file_path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, obj_id, 'download', '', use_onetime=True
    )
    url = gen_inner_file_get_url(token, file_name + '.csv')
    content = requests.get(url).content.decode('utf-8-sig')

    file_size = sys.getsizeof(content)
    dtable_io_logger.info('csv file size: %d KB' % (file_size >> 10))
    from io import StringIO
    return StringIO(content)


def get_rows_from_dtable_server(username, dtable_uuid, table_name):
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/?' + 'table_name=' + table_name + \
          '&convert_link_id=true&from=dtable_events'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}

    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        raise ConnectionError('failed to get rows %s %s' % (dtable_uuid, res.text))
    return json.loads(res.content.decode()).get('rows', [])


def update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name):
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-update-rows/?from=dtable_events'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
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
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/batch-append-rows/?from=dtable_events'
    dtable_server_access_token = get_dtable_server_token(username, dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
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
    filenames = [file_name + '.xlsx', file_name + '.json', file_name + '.csv']
    seafile_api.del_file(repo_id, EXCEL_DIR_PATH, json.dumps(filenames), username)


def get_metadata_from_dtable_server(dtable_uuid, username, permission):
    # generate json web token
    # internal usage exp 60 seconds, username = request.user.username

    DTABLE_PRIVATE_KEY = str(task_manager.conf['dtable_private_key'])
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/metadata/?from=dtable_events'

    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': username,
        'permission': permission,
    }
    access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')

    # 1. get cols from dtable-server
    headers = {'Authorization': 'Token ' + access_token}
    res = requests.get(url, headers=headers)

    if res.status_code != 200:
        raise ConnectionError('failed to get metadata %s %s' % (dtable_uuid, res.text))
    return json.loads(res.content)['metadata']


def get_view_rows_from_dtable_server(dtable_uuid, table_id, view_id, username, id_in_org, permission, table_name, view_name):
    DTABLE_PRIVATE_KEY = str(task_manager.conf['dtable_private_key'])
    api_url = get_inner_dtable_server_url()
    url = api_url.rstrip('/') + '/api/v1/internal/dtables/' + dtable_uuid + '/view-rows/?from=dtable_events'

    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'table_id': table_id,
        'view_id': view_id,
        'username': username,
        'id_in_org': id_in_org,
        'permission': permission,
    }
    access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')

    # 1. get cols from dtable-server
    headers = {'Authorization': 'Token ' + access_token}

    query_param = {
        'table_name': table_name,
        'view_name': view_name,
        'convert_link_id': True,
    }

    res = requests.get(url, headers=headers, params=query_param)

    if res.status_code != 200:
        raise Exception(res.json().get('error_msg'))
    return res.json()


def convert_db_rows(metadata, results):
    """ Convert dtable-db rows data to readable rows data
    :param metadata: list
    :param results: list
    :return: list
    """
    from dtable_events.dtable_io import dtable_io_logger
    converted_results = []
    column_map = {column['key']: column for column in metadata}
    select_map = {}
    for column in metadata:
            column_type = column['type']
            if column_type in ('single-select', 'multiple-select'):
                column_data = column['data']
                if not column_data:
                    continue
                column_key = column['key']
                column_options = column['data']['options']
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
                elif column_type == 'date' and value:
                    try:
                        date_value = parser.isoparse(value)
                        date_format = column['data']['format']
                        if date_format == 'YYYY-MM-DD':
                            value = date_value.strftime('%Y-%m-%d')
                        else:
                            value = date_value.strftime('%Y-%m-%d %H:%M')
                    except Exception as e:
                        pass
                    item[column_name] = value
                else:
                    item[column_name] = value
            else:
                item[column_key] = value
        converted_results.append(item)

    return converted_results


def get_related_nicknames_from_dtable(dtable_uuid, username, permission):
    DTABLE_PRIVATE_KEY = str(task_manager.conf['dtable_private_key'])
    url = task_manager.conf['dtable_web_service_url'].strip('/') + '/api/v2.1/dtables/%s/related-users/' % dtable_uuid

    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': username,
        'permission': permission,
    }
    access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
    headers = {'Authorization': 'Token ' + access_token}

    res = requests.get(url, headers=headers)

    if res.status_code != 200:
        raise ConnectionError('failed to get related users %s %s' % (dtable_uuid, res.text))
    return res.json().get('user_list')


def get_nicknames_from_dtable(user_id_list):
    DTABLE_PRIVATE_KEY = str(task_manager.conf['dtable_private_key'])
    url = task_manager.conf['dtable_web_service_url'].strip('/') + '/api/v2.1/users-common-info/'

    payload = {
        'exp': int(time.time()) + 60
    }
    access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
    headers = {'Authorization': 'Token ' + access_token}

    json_data = {
                'user_id_list': user_id_list,
            }
    res = requests.post(url, headers=headers, json=json_data)

    if res.status_code != 200:
        raise ConnectionError('failed to get users %s' % res.text)

    return res.json().get('user_list')

def sync_app_users_to_table(dtable_uuid, app_id, table_name, table_id, username, db_session):
    from dtable_events.utils.dtable_server_api import DTableServerAPI
    api_url = get_inner_dtable_server_url()
    base = DTableServerAPI(username, dtable_uuid, api_url)
    user_list = get_app_users(db_session, app_id)
    # handle the sync logic
    metadata = base.get_metadata()

    # handle table
    tables = metadata.get('tables', [])
    table = None
    for t in tables:
        if t.get('_id') == table_id:
            table = t
            break
        if t.get('name') == table_name:
            table = t
            break


    if not table:
        new_columns = []
        for col_name, col_type in APP_USERS_COUMNS_TYPE_MAP.items():
            col_info = {'column_name': col_name, 'column_type': col_type}
            if col_type == ColumnTypes.DATE:
                col_info['column_data'] = {'format': "YYYY-MM-DD HH:mm"}
            new_columns.append(col_info)
        table = base.add_table(table_name, columns = new_columns)
    else:
        table_columns = table.get('columns', [])
        column_names = [col.get('name') for col in table_columns]

        column_for_create = set(APP_USERS_COUMNS_TYPE_MAP.keys()).difference(set(column_names))

        for col in column_for_create:
            column_data = None
            column_type = APP_USERS_COUMNS_TYPE_MAP.get(col)
            if column_type == ColumnTypes.DATE:
                column_data = {'format': "YYYY-MM-DD HH:mm"}
            try:
                base.insert_column(table['name'], col, column_type, column_data=column_data)
            except:
                continue

    rows = base.list_rows(table['name'])
    rows_name_id_map = {}
    for row in rows:
        row_user = row.get('User') and row.get('User')[0] or None
        if not row_user:
            continue
        rows_name_id_map[row_user] = row

    row_data_for_create = []
    row_data_for_update = []
    row_ids_for_delete = get_row_ids_for_delete(rows_name_id_map, user_list)
    for user_info in user_list:
        username = user_info.get('email')
        matched, op, row_id = match_user_info(rows_name_id_map, username, user_info)
        row_data = {
                "Name": user_info.get('name'),
                "User": [username, ],
                "Role": user_info.get('role_name'),
                "IsActive": True if user_info.get('is_active') else None,
                "JoinedAt": user_info.get('created_at')
            }
        if matched:
            continue
        if op == 'create':
            row_data_for_create.append(row_data)
        elif op == 'update':
            row_data_for_update.append({
                "row_id": row_id,
                "row": row_data
            })


    step = 1000
    if row_data_for_create:
        for i in range(0, len(row_data_for_create), step):
            base.batch_append_rows(table['name'], row_data_for_create[i: i+step])

    if row_data_for_update:
        for i in range(0, len(row_data_for_update), step):
            base.batch_update_rows(table['name'], row_data_for_update[i: i+step])

    if row_ids_for_delete:
        for i in range(0, len(row_ids_for_delete), step):
            base.batch_delete_rows(table['name'], row_ids_for_delete[i: i+step])

    if row_data_for_create or row_data_for_update or row_ids_for_delete:
        update_app_sync(db_session, app_id, table['_id'])
