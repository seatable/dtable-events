import json

import requests
import os
import time
import logging
import io
import uuid
import datetime
import random
import stat
import string
import sys
import re
import hashlib
import shutil
from zipfile import ZipFile, is_zipfile
from uuid import UUID, uuid4
from urllib.parse import quote as urlquote
import posixpath

from sqlalchemy import text
from seaserv import seafile_api, USE_GO_FILESERVER

from dtable_events.app.config import INNER_DTABLE_DB_URL, INNER_DTABLE_SERVER_URL, DTABLE_WEB_SERVICE_URL, INNER_FILE_SERVER_ROOT
from dtable_events.dtable_io.external_app import APP_USERS_COUMNS_TYPE_MAP, match_user_info, update_app_sync, \
    get_row_ids_for_delete, get_app_users
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.utils import uuid_str_to_36_chars, get_inner_fileserver_root, \
    gen_file_get_url, gen_file_upload_url, uuid_str_to_32_chars

from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.exception import BaseSizeExceedsLimitError

# this two prefix used in exported zip file
FILE_URL_PREFIX = 'file://dtable-bundle/asset/files/'
EXTERNAL_APPS_FILE_URL_PREFIX = 'file://dtable-bundle-external-apps/asset/files/'
IMG_URL_PREFIX = 'file://dtable-bundle/asset/images/'
EXTERNAL_APPS_IMG_URL_PREFIX = 'file://dtable-bundle-external-apps/asset/images/'
DTABLE_IO_DIR = '/tmp/dtable-io/'

# image path: /${docUuid}/images/${filename}
SDOC_IMAGES_DIR = '/images/'
DOCUMENT_PLUGIN_FILE_RELATIVE_PATH = 'files/plugins/document'
DOCUMENT_CONFIG_FILE_NAME = 'documents.json'

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


def gen_dir_zip_download_url(token):
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/zip/%s' % (INNER_FILE_SERVER_ROOT, token)

def get_file_download_url(token, filename):
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/files/%s/%s' % (INNER_FILE_SERVER_ROOT, token,
                               urlquote(filename))


def convert_dtable_export_file_and_image_url(workspace_id, dtable_uuid, dtable_content):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded
    """
    from dtable_events.dtable_io import dtable_io_logger

    tables = dtable_content.get('tables', [])
    settings = dtable_content.get('settings')
    if settings:
        dtable_content['settings']['enable_archive'] = False
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
                            if '/external-apps/' in item:
                                v[idx] = EXTERNAL_APPS_IMG_URL_PREFIX + img_name
                            else:
                                v[idx] = IMG_URL_PREFIX + img_name
                elif col['type'] == ColumnTypes.FILE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, dict) and old_file_part_path in item.get('url', ''):
                            file_name = '/'.join(item['url'].split('/')[-2:])
                            if '/external-apps/' in item.get('url', ''):
                                item['url'] = EXTERNAL_APPS_FILE_URL_PREFIX + file_name
                            else:
                                item['url'] = FILE_URL_PREFIX + file_name

                elif col['type'] == ColumnTypes.LONG_TEXT and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        if old_file_part_path in item:
                            img_name = '/'.join(item.split('/')[-2:])
                            if '/external-apps/' in item:
                                v['images'][idx] = EXTERNAL_APPS_IMG_URL_PREFIX + img_name
                                v['text'] = v['text'].replace(item, v['images'][idx])
                            else:
                                v['images'][idx] = IMG_URL_PREFIX + img_name
                                v['text'] = v['text'].replace(item, v['images'][idx])
    return dtable_content


def log_dtable_info(dtable_uuid, content, username, op_type):
    from dtable_events.dtable_io import dtable_io_logger

    try:
        tables = content.get('tables') or []
        rows_count = 0
        for table in tables:
            table_rows_count = len(table.get('rows') or [])
            dtable_io_logger.info('%s username: %s dtable: %s table: %s name: %s rows: %s', op_type, username , dtable_uuid, table.get('_id'), table.get('name'), table_rows_count)
            rows_count += table_rows_count
        dtable_io_logger.info('%s username: %s dtable: %s total rows count: %s', op_type, username, dtable_uuid, rows_count)
    except Exception as e:
        dtable_io_logger.exception('%s username: %s dtable: %s error: %s', op_type, username, dtable_uuid, e)


def prepare_dtable_json_from_memory(workspace_id, dtable_uuid, username, path):
    """
    Used in dtable file export in real-time from memory by request the api of dtable-server
    It is more effective than exporting dtable files from seafile-server which will take about 5 minutes
    for synchronizing the data from memory to seafile-server.
    :param dtable_uuid:
    :param username:
    :return:
    """
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    json_content = dtable_server_api.get_base()
    dtable_content = convert_dtable_export_file_and_image_url(workspace_id, dtable_uuid, json_content)
    log_dtable_info(dtable_uuid, dtable_content, username, 'export dtable')
    content_json = json.dumps(dtable_content).encode('utf-8')

    with open(os.path.join(path, 'content.json'), 'wb') as f:
        f.write(content_json)

def prepare_asset_files_download(username, repo_id, dtable_uuid, asset_dir_id, path, task_id):
    from dtable_events.dtable_io import dtable_io_logger, add_task_id_to_log

    export_asset_path = os.path.join(path, 'asset')
    def download_assets_files_recursively(username, repo_id, dtable_uuid, asset_dir_id, task_id, base_path=None):
        """
        Recursively download asset directory.
        1. Walk through directory entries via file server API
        2. For each file, download individually; on error, skip
        3. Skip files that already exist if resumeable_export is True
        """
        if base_path is None:
            base_path = export_asset_path

        os.makedirs(base_path, exist_ok=True)
        try:
            entries = seafile_api.list_dir_by_dir_id(repo_id, asset_dir_id)
        except Exception as e:
            dtable_io_logger.error(add_task_id_to_log(f"Failed to list dir {asset_dir_id}: {e}", task_id))
            return

        asset_path = base_path[len(export_asset_path):] or '/'
        dtable_io_logger.info(add_task_id_to_log(f"Start to download assets in {asset_path}", task_id))

        file_download_count = 0
        file_skip_count = 0
        file_fail_count = 0
        for entry in entries:
            file_name = entry.obj_name
            obj_id = entry.obj_id
            path = os.path.join(base_path, file_name)

            if stat.S_ISDIR(entry.mode):
                # recurse into subdir
                download_assets_files_recursively(username, repo_id, dtable_uuid, obj_id, task_id,
                                            base_path=path)
            else:
                # skip if already downloaded 
                if os.path.exists(path):
                    file_skip_count += 1
                    continue
                token = seafile_api.get_fileserver_access_token(
                    repo_id, obj_id, 'download', username, use_onetime=False
                )
                #ensure file downlod intact
                temp_path = path +  '.part  '
                try:
                    url = get_file_download_url(token, file_name)
                    resp = requests.get(url, stream=True)
                    resp.raise_for_status()

                    with open(temp_path, 'wb') as f:
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                    os.rename(temp_path, path)

                except Exception as e:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                    file_fail_count += 1
                    dtable_io_logger.warning(add_task_id_to_log(f"Failed to download {path}, skipping: {e}", task_id))    
                else:
                    file_download_count += 1
        dtable_io_logger.info(add_task_id_to_log(f"Downloaded {file_download_count} files, skip {file_skip_count} files, failed {file_fail_count} files in {asset_path}", task_id))
    dtable_io_logger.info(add_task_id_to_log(f"export dtable: {dtable_uuid} username: {username} start asset recursive download", task_id))
    download_assets_files_recursively(username, repo_id, dtable_uuid, asset_dir_id, task_id)
    dtable_io_logger.info(add_task_id_to_log(f"export dtable: {dtable_uuid} username: {username} asset download complete", task_id))


def prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id, path, task_id):
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
    from dtable_events.dtable_io import dtable_io_logger, add_task_id_to_log

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
    last_log_time = None
    dtable_io_logger.info(add_task_id_to_log(f'export dtable: {dtable_uuid} username: {username} start to zip assets', task_id))
    while progress['zipped'] != progress['total']:
        time.sleep(0.5)   # sleep 0.5 second
        try:
            progress = json.loads(seafile_api.query_zip_progress(token))
        except Exception as e:
            raise e
        else:
            # per 10s or zip progress done, log progress
            if not last_log_time or time.time() - last_log_time > 10 or progress['zipped'] == progress['total']:
                dtable_io_logger.info(add_task_id_to_log(f'progress {progress}', task_id))
                last_log_time = time.time()
            failed_reason = progress.get('failed_reason')
            if failed_reason:
                raise Exception(failed_reason)
    dtable_io_logger.info(add_task_id_to_log(f'export dtable: {dtable_uuid} username: {username} zip assets done', task_id))

    dtable_io_logger.info(add_task_id_to_log(f'export dtable: {dtable_uuid} username: {username} start to download asset zip', task_id))

    asset_url = gen_dir_zip_download_url(token)
    try:
        resp = requests.get(asset_url)
    except Exception as e:
        raise e
    dtable_io_logger.info(add_task_id_to_log(f'export dtable user: {username} dtable: {dtable_uuid} asset size: {len(resp.content) / 1024 / 1024} MB', task_id))
    file_obj = io.BytesIO(resp.content)
    if is_zipfile(file_obj):
        with ZipFile(file_obj) as zp:
            dtable_io_logger.info(add_task_id_to_log(f'export dtable user: {username} dtable: {dtable_uuid} start to extractall', task_id))
            zp.extractall(path)

def copy_src_forms_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = "SELECT `username`, `form_config`, `share_type` FROM dtable_forms WHERE dtable_uuid=:dtable_uuid"
    src_forms = db_session.execute(text(sql), {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
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
    # copy auto-rules
    sql = """SELECT `id`, `run_condition`, `trigger`, `actions` FROM dtable_automation_rules WHERE dtable_uuid=:dtable_uuid"""
    src_auto_rules = db_session.execute(text(sql), {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_auto_rules_json = []
    for src_auto_rule in src_auto_rules:
        auto_rule = {
            'id': src_auto_rule.id,
            'run_condition': src_auto_rule.run_condition,
            'trigger': src_auto_rule.trigger,
            'actions': src_auto_rule.actions,
        }
        src_auto_rules_json.append(auto_rule)
    if src_auto_rules_json:
        with open(os.path.join(tmp_file_path, 'auto_rules.json'), 'w+') as fp:
            fp.write(json.dumps(src_auto_rules_json))

    # copy auto-rules navigation
    sql = '''SELECT `detail` FROM `dtable_automation_rules_navigation` WHERE dtable_uuid=:dtable_uuid'''
    src_auto_rules_navigation = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
    if src_auto_rules_navigation:
        try:
            json.loads(src_auto_rules_navigation.detail)
        except:
            pass
        else:
            with open(os.path.join(tmp_file_path, 'auto_rules_navigation.json'), 'w') as fp:
                fp.write(src_auto_rules_navigation.detail)


def copy_src_workflows_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = """SELECT `token`, `workflow_config` FROM dtable_workflows WHERE dtable_uuid=:dtable_uuid"""
    src_workflows = db_session.execute(text(sql), {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_workflows_json = []
    for src_workflow in src_workflows:
        workflow = {
            'token': src_workflow[0],
            'workflow_config': src_workflow[1]
        }
        src_workflows_json.append(workflow)
    if src_workflows_json:
        with open(os.path.join(tmp_file_path, 'workflows.json'), 'w+') as fp:
            fp.write(json.dumps(src_workflows_json))


def copy_src_external_app_to_json(dtable_uuid, tmp_file_path, db_session):
    if not db_session:
        return
    sql = """SELECT `id`, `app_config` FROM dtable_external_apps WHERE dtable_uuid=:dtable_uuid"""
    src_external_apps = db_session.execute(text(sql), {'dtable_uuid': ''.join(dtable_uuid.split('-'))})
    src_external_apps_json = []
    for src_external_app in src_external_apps:
        external_app = {
            'app_config': json.loads(src_external_app.app_config),
            'id': src_external_app.id
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
    dtable_web_service_url = DTABLE_WEB_SERVICE_URL.rstrip('/')

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
                        if isinstance(item, str):
                            img_name = '/'.join(item.split('/')[-2:])
                            if IMG_URL_PREFIX in item:
                                new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                    str(UUID(dtable_uuid)), 'images', img_name])
                                v[idx] = new_url
                            if EXTERNAL_APPS_IMG_URL_PREFIX in item:
                                new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                    str(UUID(dtable_uuid)), 'external-apps', 'images', img_name])
                                v[idx] = new_url
                elif col['type'] == ColumnTypes.FILE and isinstance(v, list) and v:
                    for idx, item in enumerate(v):
                        if isinstance(item, dict):
                            url = item.get('url', '')  
                            file_name = '/'.join(url.split('/')[-2:])
                            if EXTERNAL_APPS_FILE_URL_PREFIX in url:
                                new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                    str(UUID(dtable_uuid)), 'external-apps', 'files', file_name])
                                item['url'] = new_url
                            if FILE_URL_PREFIX in url:
                                new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                    str(UUID(dtable_uuid)), 'files', file_name])
                                item['url'] = new_url
                elif col['type'] == ColumnTypes.LONG_TEXT and isinstance(v, dict) and v.get('text') and v.get('images'):
                    for idx, item in enumerate(v['images']):
                        img_name = '/'.join(item.split('/')[-2:])
                        if IMG_URL_PREFIX in item:
                            new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                str(UUID(dtable_uuid)), 'images', img_name])
                            v['images'][idx] = new_url
                            v['text'] = v['text'].replace(item, v['images'][idx])
                        if EXTERNAL_APPS_IMG_URL_PREFIX in item:
                            new_url = '/'.join([dtable_web_service_url, 'workspace', str(workspace_id), 'asset',
                                                str(UUID(dtable_uuid)), 'external-apps', 'images', img_name])
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


def get_uuid_from_custom_path(path):
    dot_index = path.find('.')
    if dot_index == -1:
        return path[len('custom-asset://'):]
    else:
        return path[len('custom-asset://'): dot_index]


def get_dtable_uuid_parent_path_md5(dtable_uuid, parent_path):
    parent_path = parent_path.strip('/')
    return hashlib.md5((dtable_uuid + '-' + parent_path).encode('utf-8')).hexdigest()


def update_custom_assets(content, dst_dtable_uuid, path, db_session):
    if not os.path.isdir(os.path.join(path, 'asset', 'custom')):
        return content
    old_new_dict = {}
    uuid_old_path_dict = {}
    # get old custom-assets in content
    for table in content['tables']:
        img_cols = [col['key'] for col in table['columns'] if col['type'] == 'image']
        file_cols = [col['key'] for col in table['columns'] if col['type'] == 'file']
        for row in table['rows']:
            for img_col in img_cols:
                if img_col in row and isinstance(row[img_col], list):
                    for img in row[img_col]:
                        if not img.startswith('custom-asset://'):
                            continue
                        uuid_old_path_dict[get_uuid_from_custom_path(img)] = img
                        old_new_dict[img] = ''
            for file_col in file_cols:
                if file_col in row and isinstance(row[file_col], list):
                    for file in row[file_col]:
                        if not file['url'].startswith('custom-asset://'):
                            continue
                        uuid_old_path_dict[get_uuid_from_custom_path(file['url'])] = file['url']
                        old_new_dict[file['url']] = ''


    # create custom-assets objects
    step = 1000
    olds = list(old_new_dict.keys())
    for i in range(0, len(olds), step):
        old_uuids = [get_uuid_from_custom_path(old).replace('-', '') for old in olds[i: i+step]]
        sql = "SELECT `uuid`, `parent_path`, `file_name` FROM `custom_asset_uuid` WHERE `uuid` IN :uuids"
        old_custom_uuids = db_session.execute(text(sql), {'uuids': old_uuids})
        custom_uuid_infos = []
        for uuid_obj in old_custom_uuids:
            file_uuid = uuid_obj.uuid
            parent_path = uuid_obj.parent_path
            file_name = uuid_obj.file_name
            new_uuid = uuid4()
            new_md5 = get_dtable_uuid_parent_path_md5(dst_dtable_uuid, parent_path)
            ext = os.path.splitext(uuid_old_path_dict[str(UUID(file_uuid))])[1]
            old_new_dict[uuid_old_path_dict[str(UUID(file_uuid))]] = f'custom-asset://{str(new_uuid)}{ext}'
            custom_uuid_infos.append({
                'dtable_uuid': dst_dtable_uuid,
                'uuid': new_uuid.hex,
                'parent_path': parent_path,
                'dtable_uuid_parent_path_md5': new_md5,
                'file_name': file_name
            })
        values_str = ', '.join(map(lambda info: "('%(dtable_uuid)s', '%(uuid)s', '%(parent_path)s', '%(dtable_uuid_parent_path_md5)s', '%(file_name)s')" % info, custom_uuid_infos))
        sql = "INSERT INTO `custom_asset_uuid` (`dtable_uuid`, `uuid`, `parent_path`, `dtable_uuid_parent_path_md5`, `file_name`) VALUES %s" % values_str
        db_session.execute(text(sql))
        db_session.commit()

    # update content
    for table in content['tables']:
        img_cols = [col['key'] for col in table['columns'] if col['type'] == 'image']
        file_cols = [col['key'] for col in table['columns'] if col['type'] == 'file']
        for row in table['rows']:
            for img_col in img_cols:
                if img_col in row and isinstance(row[img_col], list):
                    for index in range(len(row[img_col])):
                        if not row[img_col][index].startswith('custom-asset://'):
                            continue
                        row[img_col][index] = old_new_dict[row[img_col][index]]  # update
            for file_col in file_cols:
                if file_col in row and isinstance(row[file_col], list):
                    for index in range(len(row[file_col])):
                        if not row[file_col][index]['url'].startswith('custom-asset://'):
                            continue
                        row[file_col][index]['url'] = old_new_dict[row[file_col][index]['url']]  # update

    return content


def post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage, path, db_session):
    """
    used to import dtable
    prepare dtable json file and post it at file server

    :param repo_id:
    :param workspace_id:
    :param dtable_uuid:         str
    :param dtable_file_name:    xxx.dtable, the name of zip we imported
    :return:
    """
    from dtable_events.dtable_io import dtable_io_logger
    from dtable_events.utils.storage_backend import storage_backend

    # if dtable content exists, don't change anything
    dtable_content = storage_backend.get_dtable(dtable_uuid)
    if dtable_content:
        return dtable_content

    # change url in content json, then save it at file server
    content_json_file_path = os.path.join(path, 'content.json')
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

    log_dtable_info(dtable_uuid, content, username, 'import dtable')

    content_json = convert_dtable_import_file_url(content, workspace_id, dtable_uuid)

    try:
        content_json = update_custom_assets(content, dtable_uuid, path, db_session)
    except Exception as e:
        dtable_io_logger.exception('update dtable: %s update custom assets error: %s', dtable_uuid, e)

    try:
        storage_backend.save_dtable(dtable_uuid, json.dumps(content_json), username, in_storage, repo_id, dtable_file_name)
    except Exception as e:
        raise e

    return content_json


def post_asset_files(repo_id, dtable_uuid, username, path):
    """
    used to import dtable
    post asset files in  /tmp/dtable-io/<dtable_uuid>/dtable_zip_extracted/ to file server

    :return:
    """
    asset_root_path = os.path.join('/asset', dtable_uuid)

    extracted_asset_path = os.path.join(path, 'asset')
    for dirpath, dirs, files in os.walk(extracted_asset_path):
        for file_name in files:
            inner_path = dirpath[len(extracted_asset_path)+1:]  # path inside zip
            tmp_file_path = os.path.join(dirpath, file_name)
            cur_file_parent_path = os.path.join(asset_root_path, inner_path)
            # check current file's parent path before post file
            path_id = seafile_api.get_dir_id_by_path(repo_id, cur_file_parent_path)
            if not path_id:
                seafile_api.mkdir_with_parents(repo_id, '/', cur_file_parent_path[1:], username)

            cur_file_path = os.path.join(cur_file_parent_path, file_name)
            file_id = seafile_api.get_file_id_by_path(repo_id, cur_file_path)
            if file_id:
                continue

            seafile_api.post_file(repo_id, tmp_file_path, cur_file_parent_path, file_name, username)

# execute after post asset
def update_page_content(workspace_id, dtable_uuid, page_id, page_content, need_check_static=True):
    valid_dtable_web_service_url = DTABLE_WEB_SERVICE_URL.rstrip('/')
    is_changed = False
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
    if page_content.get('page_id') != page_id:
        page_content['page_id'] = page_id
        is_changed = True
    pages = page_content.get('pages', [])
    if need_check_static:
        for sub_page in pages:
            element_map = sub_page.get('element_map', {})
            for element_id in element_map:
                element = element_map.get(element_id, {})
                if element['type'] == 'static_image':
                    config_data = element.get('config_data', {})
                    static_image_url = config_data.get('staticImageUrl', '')
                    file_name = '/'.join(static_image_url.split('/')[-2:])
                    config_data['staticImageUrl'] = '/'.join([valid_dtable_web_service_url, 'workspace', str(workspace_id),
                                                            'asset', uuid_str_to_36_chars(str(dtable_uuid)), 'page-design', page_id, file_name])
                    is_changed = True
    return {
        'page_content': page_content,
        'is_changed': is_changed
    }


def update_page(repo_id, workspace_id, dtable_uuid, page, inner_file_server_root):
    page_id = page['page_id']
    page_content_file_name = '%s.json'%(page_id)
    page_content_url = page['content_url']
    page_json_file_id = seafile_api.get_file_id_by_path(repo_id, '/asset' + page_content_url.split('asset')[1])
    token = seafile_api.get_fileserver_access_token(
        repo_id, page_json_file_id, 'view', '', use_onetime=False
    )
    content_url = '%s/files/%s/%s'%(inner_file_server_root, token,
                            urlquote(page_content_file_name))
    page_content_response = requests.get(content_url)
    page_content = None
    is_changed = False
    if page_content_response.status_code == 200:
        page_content = page_content_response.json()
        info = update_page_content(workspace_id, dtable_uuid, page_id, page_content)
        page_content = info['page_content']
        is_changed = info['is_changed']
    return {
        'page_content': page_content,
        'is_changed': is_changed
    }

# page_design_settings, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, username
def update_page_design_static_image(page_design_settings, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, username):
    if not isinstance(page_design_settings, list):
        return

    inner_file_server_root = INNER_FILE_SERVER_ROOT.strip('/')
    from dtable_events.dtable_io import dtable_io_logger
    try:
        for page in page_design_settings:
            page_id = page['page_id']
            page_content_file_name = '%s.json'%(page_id)
            parent_dir = '/asset/%s/page-design/%s'%(dtable_uuid, page_id)
            info = update_page(repo_id, workspace_id, dtable_uuid, page, inner_file_server_root)
            try:
                page_content, is_changed = info['page_content'], info['is_changed']
            except Exception as e:
                dtable_io_logger.exception('')
            if is_changed:
                if not os.path.exists(content_json_tmp_path):
                    os.makedirs(content_json_tmp_path)
                page_content_save_path = os.path.join(content_json_tmp_path, page_content_file_name)
                with open(page_content_save_path, 'w') as f:
                    json.dump(page_content, f)
                seafile_api.put_file(repo_id, page_content_save_path, parent_dir, '%s.json'%(page_id), username, None)
    except Exception as e:
        dtable_io_logger.warning('update page design static image failed. ERROR: {}'.format(e))


def rename_universal_app_static_assets_dir(pages, app_id, repo_id, dtable_uuid, username):
    if not isinstance(pages, list):
            return

    from dtable_events.dtable_io import dtable_io_logger

    app_parent_dir = '/asset/%s/external-apps'%(dtable_uuid)
    for page in pages:
        page_id = page['id']
        content_url = page.get('content_url')
        if not content_url:
            continue

        # support relative path: /app_id/page_id/
        parent_dir_re = r'/\d+/%s/%s.json'%(page_id, page_id)
        if re.search(parent_dir_re, content_url):
            old_content_app_id = content_url.split('/')[1]
            content_path = '%s/%s/%s/%s.json' % (app_parent_dir, old_content_app_id, page_id, page_id)
            content_file_id = seafile_api.get_file_id_by_path(repo_id, content_path)

            # just rename the dir which content file exist
            if not content_file_id:
                continue

            try:
                seafile_api.rename_file(repo_id, app_parent_dir, '%s'%old_content_app_id, '%s'%app_id, username)
            except Exception as e:
                dtable_io_logger.warning('rename custom page\'s content dir of external app failed. ERROR: {}'.format(e))
            break

def update_universal_app_custom_page_static_image(pages, app_id, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, username):
    if not isinstance(pages, list):
        return

    custom_pages = [ page for page in pages if page.get('type', '') == 'custom_page' ]

    inner_file_server_root = INNER_FILE_SERVER_ROOT.strip('/')
    from dtable_events.dtable_io import dtable_io_logger
    try:
        for page in custom_pages:
            page_id = page['id']
            page_content_file_name = '%s.json'%(page_id)

            # /app_id/page_id/
            parent_dir = '/asset/%s/external-apps/%s/%s'%(dtable_uuid, app_id, page_id)
            file_path = parent_dir + '/' + page_content_file_name
            page_json_file_id = seafile_api.get_file_id_by_path(repo_id, file_path)
            if not page_json_file_id:
                continue

            token = seafile_api.get_fileserver_access_token(
                repo_id, page_json_file_id, 'view', '', use_onetime=False
            )
            content_url = '%s/files/%s/%s'%(inner_file_server_root, token,
                                    urlquote(page_content_file_name))
            page_content_response = requests.get(content_url)
            is_changed = False
            if page_content_response.status_code == 200:
                page_content = page_content_response.json()
                if 'block_ids' not in page_content.keys():
                    page_content = {
                        'block_ids': [],
                        'block_by_id': {},
                        'version': 5
                    }
                block_ids = page_content.get('block_ids', [])
                block_by_id = page_content.get('block_by_id', {})
                for block_id in block_ids:
                    block = block_by_id.get(block_id, {})
                    block_children = block.get('children', [])
                    for block_children_id in block_children:
                        element = block_by_id.get(block_children_id, {})
                        element_type = element.get('type', '')
                        if element_type == 'static_long_text':
                            # images in external-apps/form_long_text_image
                            dst_image_url_part = '%s/asset/%s/external-apps/form_long_text_image' % (str(workspace_id), str(dtable_uuid))
                            element['value']['text'] = re.sub(r'\d+/asset/[-0-9a-f]{36}/external-apps/form_long_text_image', dst_image_url_part, element['value']['text'])
                            # images in external-apps/<app_id>/page_id
                            dst_image_url_part = '%s/asset/%s/external-apps/%s/%s' % (str(workspace_id), str(dtable_uuid), app_id, page_id)
                            element['value']['text'] = re.sub(r'\d+/asset/[-0-9a-f]{36}/external-apps/\d+/\w+', dst_image_url_part, element['value']['text'])

                            is_changed = True

                if is_changed:
                    if not os.path.exists(content_json_tmp_path):
                        os.makedirs(content_json_tmp_path)
                    page_content_save_path = os.path.join(content_json_tmp_path, page_content_file_name)
                    with open(page_content_save_path, 'w') as f:
                        json.dump(page_content, f)
                    seafile_api.put_file(repo_id, page_content_save_path, parent_dir, '%s.json'%(page_id), username, None)
    except Exception as e:
        dtable_io_logger.warning('update custom page\'s static image of external app failed. ERROR: {}'.format(e))


def update_universal_app_single_record_page_static_assets(pages, app_id, repo_id, workspace_id, dtable_uuid, content_json_tmp_path, username):
    if not isinstance(pages, list):
        return

    single_record_pages = [ page for page in pages if page.get('type', '') == 'single_record_page' ]

    inner_file_server_root = INNER_FILE_SERVER_ROOT.strip('/')
    from dtable_events.dtable_io import dtable_io_logger
    try:
        for page in single_record_pages:
            page_id = page['id']
            page_content_file_name = '%s.json'%(page_id)

            # /app_id/page_id/
            parent_dir = '/asset/%s/external-apps/%s/%s'%(dtable_uuid, app_id, page_id)
            file_path = parent_dir + '/' + page_content_file_name
            page_json_file_id = seafile_api.get_file_id_by_path(repo_id, file_path)
            if not page_json_file_id:
                continue

            token = seafile_api.get_fileserver_access_token(
                repo_id, page_json_file_id, 'view', '', use_onetime=False
            )
            content_url = '%s/files/%s/%s'%(inner_file_server_root, token,
                                    urlquote(page_content_file_name))
            page_content_response = requests.get(content_url)
            is_changed = False
            if page_content_response.status_code == 200:
                page_content = page_content_response.json()
                elements = page_content.get('elements', [])
                for element in elements:
                    element_type = element.get('type', '')
                    if element_type == 'long_text':
                        # images in external-apps/form_long_text_image
                        dst_image_url_part = '%s/asset/%s/external-apps/form_long_text_image' % (str(workspace_id), str(dtable_uuid))
                        element['value']['text'] = re.sub(r'\d+/asset/[-0-9a-f]{36}/external-apps/form_long_text_image', dst_image_url_part, element['value']['text'])
                        # images in external-apps/<app_id>/page_id
                        dst_image_url_part = '%s/asset/%s/external-apps/%s/%s' % (str(workspace_id), str(dtable_uuid), app_id, page_id)
                        element['value']['text'] = re.sub(r'\d+/asset/[-0-9a-f]{36}/external-apps/\d+/\w+', dst_image_url_part, element['value']['text'])

                        is_changed = True

                if is_changed:
                    if not os.path.exists(content_json_tmp_path):
                        os.makedirs(content_json_tmp_path)
                    page_content_save_path = os.path.join(content_json_tmp_path, page_content_file_name)
                    with open(page_content_save_path, 'w') as f:
                        json.dump(page_content, f)
                    seafile_api.put_file(repo_id, page_content_save_path, parent_dir, '%s.json'%(page_id), username, None)
    except Exception as e:
        dtable_io_logger.warning('update single record page\'s static assets of external app failed. ERROR: {}'.format(e))


def gen_form_id(length=4):
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(length))


def add_a_form_to_db(form, workspace_id, dtable_uuid, db_session):
    # check form id
    form_id = gen_form_id()
    sql_check_form_id = 'SELECT `id` FROM dtable_forms WHERE form_id=:form_id'
    while db_session.execute(text(sql_check_form_id), {'form_id': form_id}).rowcount > 0:
        form_id = gen_form_id()

    sql = "INSERT INTO dtable_forms (`username`, `workspace_id`, `dtable_uuid`, `form_id`, `form_config`, `token`, `share_type`, `created_at`)" \
        "VALUES (:username, :workspace_id, :dtable_uuid, :form_id, :form_config, :token, :share_type, :created_at)"

    db_session.execute(text(sql), {
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


def add_a_auto_rule_to_db(username, auto_rule, workspace_id, repo_id, owner, org_id, dtable_uuid, old_new_workflow_token_dict, db_session):
    # get org_id
    sql_get_org_id = """SELECT `org_id` FROM workspaces WHERE id=:id"""
    org_id = [x[0] for x in db_session.execute(text(sql_get_org_id), {'id': workspace_id})][0]
    try:
        actions = json.loads(auto_rule.get('actions'))
    except:
        actions = []
    for action in actions:
        if action.get('type') == 'run_python_script':
            action['workspace_id'] = int(workspace_id)
            action['owner'] = owner
            action['org_id'] = int(org_id)
            action['repo_id'] = repo_id
        elif action.get('type') == 'trigger_workflow':
            action['token'] = old_new_workflow_token_dict.get(action.get('token'))
        elif action.get('type') == 'send_email':
            action['repo_id'] = repo_id
        elif action.get('type') == 'convert_page_to_pdf':
            action['repo_id'] = repo_id
            action['workspace_id'] = int(workspace_id)
        elif action.get('type') == 'convert_document_to_pdf_and_send':
            action['repo_id'] = repo_id
            action['workspace_id'] = int(workspace_id)

    sql = """INSERT INTO dtable_automation_rules (`dtable_uuid`, `run_condition`, `trigger`, `actions`,
             `creator`, `ctime`, `org_id`, `last_trigger_time`) VALUES (:dtable_uuid, :run_condition,
             :trigger, :actions, :creator, :ctime, :org_id, :last_trigger_time)"""
    result = db_session.execute(text(sql), {
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'run_condition': auto_rule['run_condition'],
        'trigger': auto_rule['trigger'],
        'actions': json.dumps(actions),
        'creator': username,
        'ctime': datetime.datetime.utcnow(),
        'org_id': org_id,
        'last_trigger_time': None,
        })
    db_session.commit()
    return result.lastrowid


def add_a_workflow_to_db(username, workflow, workspace_id, repo_id, dtable_uuid, owner, org_id, old_new_workflow_token_dict, db_session):
    try:
        old_token = workflow.get('token')
        workflow_config = json.loads(workflow.get('workflow_config'))
    except:
        workflow_config = {}
    nodes = workflow_config.get('nodes') or []
    for node in nodes:
        actions = node.get('actions') or []
        for action in actions:
            if action.get('type') == 'run_python_script':
                action['workspace_id'] = int(workspace_id)
                action['owner'] = owner
                action['org_id'] = int(org_id)
                action['repo_id'] = repo_id

    sql = """INSERT INTO dtable_workflows (`token`,`dtable_uuid`, `workflow_config`, `creator`, `created_at`,
             `owner`) VALUES (:token, :dtable_uuid, :workflow_config,
             :creator, :created_at, :owner)"""
    new_token = str(uuid.uuid4())
    db_session.execute(text(sql), {
        'token': new_token,
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'workflow_config': json.dumps(workflow_config),
        'creator': username,
        'created_at': datetime.datetime.utcnow(),
        'owner': owner,
        })
    db_session.commit()
    old_new_workflow_token_dict[old_token] = new_token

def add_an_external_app_to_db(username, external_app, dtable_uuid, db_session, org_id, workspace_id, repo_id):
    sql = """INSERT INTO dtable_external_apps (`app_uuid`,`dtable_uuid`,`app_type`, `app_config`, `creator`, `created_at`, `org_id`)
                VALUES (:app_uuid, :dtable_uuid, :app_type, :app_config, :creator, :created_at, :org_id)"""
    app_uuid = str(uuid.uuid4())
    app_type = external_app['app_config'].get('app_type')
    db_session.execute(text(sql), {
        'app_uuid': app_uuid,
        'dtable_uuid': ''.join(dtable_uuid.split('-')),
        'app_type': app_type,
        'app_config': json.dumps(external_app['app_config']),
        'creator': username,
        'created_at': datetime.datetime.now(),
        'org_id': org_id
        })
    db_session.commit()

    # add app role as defualt
    if app_type == 'universal-app':
        sql_app_id = """SELECT `id` FROM dtable_external_apps WHERE app_uuid=:app_uuid"""
        app_id = [x[0] for x in db_session.execute(text(sql_app_id), {'app_uuid': app_uuid})][0]
        sql_app_default_role = """INSERT INTO dtable_app_roles (`app_id`,`role_name`,`role_permission`, `created_at`)
                        VALUES (:app_id, :role_name, :role_permission, :created_at)"""
        db_session.execute(text(sql_app_default_role), {
            'app_id': app_id,
            'role_name': 'default',
            'role_permission': 'rw',
            'created_at': datetime.datetime.now(),
        })
        db_session.commit()
        app_settings = external_app['app_config'].get('settings') or {}
        pages = app_settings.get('pages') or []
        rename_universal_app_static_assets_dir(pages, app_id, repo_id, dtable_uuid, username)
        os.makedirs(DTABLE_IO_DIR, exist_ok=True)
        update_universal_app_custom_page_static_image(pages, app_id, repo_id, workspace_id,
                dtable_uuid, DTABLE_IO_DIR, username)
        update_universal_app_single_record_page_static_assets(pages, app_id, repo_id, workspace_id,
                dtable_uuid, DTABLE_IO_DIR, username)
        for page in pages:
            page_type = page.get('type', '')
            page_id = page.get('id', '')
            if page_type == 'custom_page' or page_type == 'single_record_page':
                page['content_url'] = '/%s/%s/%s.json' % (app_id, page_id, page_id)
        sql = "UPDATE `dtable_external_apps` SET `app_config`=:app_config WHERE `id`=:id"
        db_session.execute(text(sql), {'app_config': json.dumps(external_app['app_config']), 'id': app_id})
        db_session.commit()


def create_forms_from_src_dtable(workspace_id, dtable_uuid, path, db_session):
    if not db_session:
        return
    forms_json_path = os.path.join(path, 'forms.json')
    if not os.path.exists(forms_json_path):
        return

    sql = "SELECT COUNT(1) as count FROM dtable_forms WHERE dtable_uuid=:dtable_uuid"
    result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
    if result.count > 0:
        return

    with open(forms_json_path, 'r') as fp:
        forms_json = fp.read()
    forms = json.loads(forms_json)
    for form in forms:
        if ('username' not in form) or ('form_config' not in form) or ('share_type' not in form):
            continue
        add_a_form_to_db(form, workspace_id, dtable_uuid, db_session)


def create_auto_rules_from_src_dtable(username, workspace_id, repo_id, owner, org_id, dtable_uuid, old_new_workflow_token_dict, path, db_session):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    if not db_session:
        return
    auto_rules_json_path = os.path.join(path, 'auto_rules.json')
    if not os.path.exists(auto_rules_json_path):
        return

    sql = "SELECT COUNT(1) as count FROM dtable_automation_rules WHERE dtable_uuid=:dtable_uuid"
    result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
    if result.count > 0:
        return

    # create auto-rules
    src_des_id_dict = {}
    with open(auto_rules_json_path, 'r') as fp:
        auto_rules_json = fp.read()
    auto_rules = json.loads(auto_rules_json)
    for auto_rule in auto_rules:
        if ('run_condition' not in auto_rule) or ('trigger' not in auto_rule) or ('actions' not in auto_rule):
            continue
        new_rule_id = add_a_auto_rule_to_db(username, auto_rule, workspace_id, repo_id, owner, org_id, dtable_uuid, old_new_workflow_token_dict, db_session)
        src_des_id_dict[auto_rule['id']] = new_rule_id
    dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, INNER_DTABLE_SERVER_URL)
    dtable_server_api.send_signal('automation-rules-changed')

    # create auto-rules navigation
    auto_rules_navigation_json_path = os.path.join(path, 'auto_rules_navigation.json')
    if os.path.isfile(auto_rules_navigation_json_path):
        with open(auto_rules_navigation_json_path, 'r') as fp:
            try:
                nav_detail = json.load(fp)
            except:
                pass
            else:
                queue = [item for item in nav_detail['children']]
                while queue:
                    item = queue.pop(0)
                    if item['type'] == 'automation_rule':
                        item['id'] = src_des_id_dict[item['id']]
                    elif item['type'] == 'folder':
                        queue.extend(item.get('children', []))
                sql = '''INSERT INTO dtable_automation_rules_navigation(`dtable_uuid`, `detail`) VALUES (:dtable_uuid, :detail)'''
                db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid), 'detail': json.dumps(nav_detail)})
                db_session.commit()


def create_workflows_from_src_dtable(username, workspace_id, repo_id, dtable_uuid, owner, org_id, old_new_workflow_token_dict, path, db_session):
    if not db_session:
        return
    workflows_json_path = os.path.join(path, 'workflows.json')
    if not os.path.exists(workflows_json_path):
        return

    sql = "SELECT COUNT(1) as count FROM dtable_workflows WHERE dtable_uuid=:dtable_uuid"
    result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
    if result.count > 0:
        return

    with open(workflows_json_path, 'r') as fp:
        workflows_json = fp.read()
    workflows = json.loads(workflows_json)
    for workflow in workflows:
        if 'workflow_config' not in workflow:
            continue
        add_a_workflow_to_db(username, workflow, workspace_id, repo_id, dtable_uuid, owner, org_id, old_new_workflow_token_dict, db_session)


def create_external_apps_from_src_dtable(username, dtable_uuid, db_session, org_id, workspace_id, repo_id, path):
    from dtable_events.dtable_io.import_table_from_base import trans_page_content_url
    if not db_session:
        return
    external_apps_json_path = os.path.join(path, 'external_apps.json')
    if not os.path.exists(external_apps_json_path):
        return

    sql = "SELECT COUNT(1) as count FROM dtable_external_apps WHERE dtable_uuid=:dtable_uuid"
    result = db_session.execute(text(sql), {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
    if result.count > 0:
        return

    with open(external_apps_json_path, 'r') as fp:
        external_apps_json = fp.read()
    external_apps = json.loads(external_apps_json)

    for external_app in external_apps:
        if 'app_config' not in external_app:
            continue
        app_id = external_app['id']
        app_config = external_app['app_config']
        if app_config['app_type'] == 'universal-app':
             settings = app_config.get('settings', {})
             pages = settings.get('pages', [])
             for page in pages:
                page_id = page.get('id', '')
                page_type = page.get('type', '')
                if page_type == 'custom_page' or page_type == 'single_record_page':
                    page['content_url'] = '/%s/%s/%s.json' % (app_id, page_id, page_id)
        add_an_external_app_to_db(username, external_app, dtable_uuid, db_session, org_id, workspace_id, repo_id)


def download_files_to_path(username, repo_id, dtable_uuid, files, path, db_session, files_map=None):
    """
    download dtable's asset files to path
    """
    from dtable_events.dtable_io import dtable_io_logger

    valid_file_obj_ids = []
    custom_uuids = []
    valid_custom_file_obj_ids = []

    # query system files
    base_path = os.path.join('/asset', dtable_uuid)
    for file in files:
        if not file.startswith('custom-asset://'):
            full_path = os.path.join(base_path, *file.split('/'))
            obj_id = seafile_api.get_file_id_by_path(repo_id, full_path)
            if not obj_id:
                continue
            valid_file_obj_ids.append((file, obj_id))
        else:
            custom_uuid = file[len('custom-asset://'): len('custom-asset://')+36].replace('-', '')
            custom_uuids.append(custom_uuid)
            if files_map:
                files_map[custom_uuid] = files_map.pop(file, None)

    if custom_uuids:
        # query custom files
        sql = "SELECT uuid, parent_path, file_name FROM custom_asset_uuid WHERE uuid IN :uuids AND dtable_uuid=:dtable_uuid"
        try:
            results = db_session.execute(text(sql), {'uuids': custom_uuids, 'dtable_uuid': uuid_str_to_36_chars(dtable_uuid)})
            for row in results:
                full_path = os.path.join(base_path, 'custom', row.parent_path, row.file_name)
                obj_id = seafile_api.get_file_id_by_path(repo_id, full_path)
                if not obj_id:
                    continue
                valid_custom_file_obj_ids.append((row.uuid, obj_id, row.file_name))
        except Exception as e:
            dtable_io_logger.error('query dtable: %s custom uuids error: %s', dtable_uuid, e)

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

    for custom_uuid, obj_id, file_name in valid_custom_file_obj_ids:
        token = seafile_api.get_fileserver_access_token(
            repo_id, obj_id, 'download', username,
            use_onetime=False
        )
        if files_map and files_map.get(custom_uuid):
            file_name = files_map.get(custom_uuid)
        file_url = gen_inner_file_get_url(token, file_name)
        content = requests.get(file_url).content
        filename_by_path = os.path.join(path, file_name)
        with open(filename_by_path, 'wb') as f:
            f.write(content)
        tmp_file_list.append(filename_by_path)
    return tmp_file_list


def upload_excel_json_to_dtable_server(username, dtable_uuid, json_file, lang='en'):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    dtable_server_api.import_excel(json_file, lang=lang)

def upload_excel_json_add_table_to_dtable_server(username, dtable_uuid, json_file, lang='en'):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    dtable_server_api.import_excel_add_table(json_file, lang=lang)


def append_rows_by_dtable_server(dtable_server_api, rows_data, table_name):
    offset = 0
    while True:
        rows = rows_data[offset: offset + 1000]
        offset = offset + 1000
        if not rows:
            break
        dtable_server_api.batch_append_rows(table_name, rows)
        time.sleep(0.5)


def get_csv_file(file_path):
    from dtable_events.dtable_io import dtable_io_logger

    # The specified character set is utf-8-sig to automatically process BOM characters
    # and eliminate the occurrence of \ ufeff
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        content = f.read()

    file_size = sys.getsizeof(content)
    dtable_io_logger.info('csv file size: %d KB' % (file_size >> 10))

    return content


def get_rows_from_dtable_server(username, dtable_uuid, table_name):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    rows = dtable_server_api.list_table_rows(table_name, convert_link_id=True)
    return rows


def update_rows_by_dtable_server(username, dtable_uuid, update_rows, table_name):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    offset, limit = 0, 1000
    while True:
        rows = update_rows[offset: offset + limit]
        offset += limit
        if not rows:
            break
        dtable_server_api.batch_update_rows(table_name, rows)
        time.sleep(0.5)


def get_metadata_from_dtable_server(dtable_uuid, username):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    metadata = dtable_server_api.get_metadata()
    return metadata


def get_view_rows_from_dtable_server(dtable_uuid, table_name, view_name, username, id_in_org, user_department_ids_map, permission):
    from dtable_events.utils.dtable_server_api import DTableServerAPI

    kwargs = {
        'id_in_org': id_in_org,
        'user_department_ids_map': user_department_ids_map
    }
    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL, permission=permission, kwargs=kwargs)
    rows = dtable_server_api.list_view_rows(table_name, view_name, convert_link_id=True)
    return rows


def get_related_nicknames_from_dtable(dtable_uuid):
    from dtable_events.utils.dtable_web_api import DTableWebAPI

    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
    related_users = dtable_web_api.get_related_users(dtable_uuid)
    user_list = related_users['user_list']
    app_user_list = related_users['app_user_list']
    results = []
    if app_user_list:
        user_list.extend(app_user_list)

    for user in user_list:
        if user in results:
            continue
        results.append(user)
    return results

def get_nicknames_from_dtable(user_id_list):
    from dtable_events.utils.dtable_web_api import DTableWebAPI

    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
    user_list = dtable_web_api.get_users_common_info(user_id_list)['user_list']
    return user_list

def sync_app_users_to_table(dtable_uuid, app_id, table_name, table_id, username, db_session):
    from dtable_events.utils.dtable_server_api import DTableServerAPI
    from dtable_events.utils.dtable_db_api import DTableDBAPI

    base = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
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

    rows_name_id_map = {}
    dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)
    query_column_names = ', '.join([f"`{column_name}`" for column_name in ['_id'] + list(APP_USERS_COUMNS_TYPE_MAP.keys())])
    offset, limit = 0, 1000
    while True:
        sql = f"SELECT {query_column_names} FROM `{table['name']}` LIMIT {offset}, {limit}"
        rows, _ = dtable_db_api.query(sql, convert=True, server_only=True)
        for row in rows:
            row_user = row.get('User') and row.get('User')[0] or None
            if not row_user:
                continue
            rows_name_id_map[row_user] = row
        if len(rows) < limit:
            break
        offset += limit

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

def to_python_boolean(string):
    """Convert a string to boolean.
    """
    string = string.lower()
    if string in ('t', 'true', '1'):
        return True
    if string in ('f', 'false', '0'):
        return False
    raise ValueError("Invalid boolean value: '%s'" % string)


def get_rows_from_dtable_db(dtable_db_api, table_name, limit=50000):
    from dtable_events.utils.dtable_db_api import convert_db_rows
    offset = 10000
    start = 0
    dtable_rows = []
    while True:
        # exported row number should less than limit
        if (start + offset) > limit:
            offset = limit - start

        sql = f"SELECT * FROM `{table_name}` LIMIT {start}, {offset}"

        response_rows, metadata = dtable_db_api.query(sql, convert=False, server_only=True)
        response_rows = convert_db_rows(metadata, response_rows)
        dtable_rows.extend(response_rows)

        start += offset
        if start >= limit or len(response_rows) < offset:
            break

    return dtable_rows


def update_rows_by_dtable_db(dtable_db_api, update_rows, table_name):
    offset = 0
    while True:
        rows = update_rows[offset: offset + 1000]
        offset = offset + 1000
        if not rows:
            break
        dtable_db_api.batch_update_rows(table_name, rows)
        time.sleep(0.5)


def extract_select_options(rows, column_name_to_column):
    select_column_options = {}
    for row in rows:
        # get column options for adding single-select or multiple-select columns
        for col_name in row:
            col_type = column_name_to_column.get(col_name, {}).get('type')
            cell_value = row.get(col_name)
            if not cell_value:
                continue
            if col_type in ['multiple-select', 'single-select']:
                col_options = select_column_options.get(col_name, set())
                if not col_options:
                    select_column_options[col_name] = col_options
                if col_type == 'multiple-select':
                    for op in cell_value:
                        if op:
                            col_options.add(op)
                else:
                    col_options.add(cell_value)

    return select_column_options

def width_transfer(pixel):

    # convert pixel of seatable to excel width
    # the default width of excel is 8.38 (width of "0" in font size of 11) which is 72px

    return round((pixel * 8.38) / 72, 2)

def height_transfer(base_row_height='default'):
    # convert pixel of seatable height to excel height
    # the default unit of height in excel is 24 pixel, which is 14.4 pound
    height_dict = {
        'default': 1,
        'double':  2,
        'triple': 3,
        'quadruple': 4
    }

    row_height_mul = (height_dict.get(base_row_height, 1))

    return round((32 * row_height_mul * 14.4 ) / 24, 2)

def zip_big_data_screen(username, repo_id, dtable_uuid, page_id, tmp_file_path):
    from dtable_events.utils import uuid_str_to_36_chars, normalize_file_path, gen_file_get_url
    base_dir = '/asset/' + dtable_uuid
    big_data_file_path = 'files/plugins/big-data-screen/%(page_id)s/%(page_id)s.json' % ({
            'page_id': page_id,
        })
    big_data_poster_path = 'files/plugins/big-data-screen/%(page_id)s/%(page_id)s.png' % ({
        'page_id': page_id
    })

    asset_path = "%s/%s" % (base_dir, big_data_file_path)

    poster_asset_path = "%s/%s" % (base_dir, big_data_poster_path)

    # 1. get the json file and poster of big-data-screen page
    #   a. json file
    asset_id = seafile_api.get_file_id_by_path(repo_id, asset_path)
    token = seafile_api.get_fileserver_access_token(
        repo_id, asset_id, 'view', '', use_onetime=False
    )
    asset_name = os.path.basename(normalize_file_path(big_data_file_path))
    url = gen_file_get_url(token, asset_name)

    resp = requests.get(url)
    page_json = json.loads(resp.content)


    #  b. poster

    poster_asset_id = seafile_api.get_file_id_by_path(repo_id, poster_asset_path)
    poster_token = seafile_api.get_fileserver_access_token(
        repo_id, poster_asset_id, 'view', '', use_onetime=False
    )
    poster_name = os.path.basename(normalize_file_path(big_data_poster_path))
    url = gen_file_get_url(poster_token, poster_name)
    resp_poster = requests.get(url)

    # 2. get the image infos in big-data-screen
    page_bg_custom_image_list = page_json.get('page_bg_custom_image_list')
    page_images = []
    page_elements = page_json.get('page_elements') or {}
    element_map = page_elements.get('element_map') or {}
    for key, value in element_map.items():
        if value.get('element_type') == 'image':
            image_url = value.get('config',{}).get('imageUrl')
            if "?" not in image_url:
                page_images.append(image_url)



    content_json = {
        'page_content': page_json,
        'page_images': page_images
    }

    # 3. write json file to tmp_file_path , write images to tmp_file_path/images
    content_json_save_path = tmp_file_path.rstrip('/')
    image_save_path = os.path.join(content_json_save_path, 'images')
    with open("%s/content.json" % content_json_save_path, 'wb') as f:
        f.write(json.dumps(content_json).encode('utf-8'))
    with open("%s/content.png" % content_json_save_path, 'wb') as f:
        f.write(resp_poster.content)

    for image_url in page_bg_custom_image_list:
        target_path = normalize_file_path(os.path.join(base_dir, image_url.strip('/')))
        asset_id = seafile_api.get_file_id_by_path(repo_id, target_path)
        if not asset_id:
            continue
        token = seafile_api.get_fileserver_access_token(
            repo_id, asset_id, 'download', username, use_onetime=False
        )
        image_name = os.path.basename(normalize_file_path(image_url))
        url = gen_file_get_url(token, image_name)

        resp = requests.get(url)
        with open('%s/%s' % (image_save_path, image_name), 'wb') as f:
            f.write(resp.content)

    for image_url in page_images:
        target_path = normalize_file_path(os.path.join(base_dir, image_url.strip('/')))
        asset_id = seafile_api.get_file_id_by_path(repo_id, target_path)
        if not asset_id:
            continue

        token = seafile_api.get_fileserver_access_token(
            repo_id, asset_id, 'download', username, use_onetime=False
        )
        image_name = os.path.basename(normalize_file_path(image_url))
        url = gen_file_get_url(token, image_name)

        resp = requests.get(url)
        with open('%s/%s' % (image_save_path, image_name), 'wb') as f:
            f.write(resp.content)

def post_big_data_screen_zip_file(username, repo_id, dtable_uuid, page_id, tmp_extracted_path):

    content_json_file_path = os.path.join(tmp_extracted_path, 'content.json')
    content_poster_file_path = os.path.join(tmp_extracted_path, 'content.png')
    new_content_poster_file_path = os.path.join(tmp_extracted_path, '%s.png' % page_id)
    poster_file_name = os.path.basename(new_content_poster_file_path)
    os.rename(content_poster_file_path, new_content_poster_file_path)
    image_path = os.path.join(tmp_extracted_path, 'images/')
    with open(content_json_file_path, 'r') as f:
        content_json = f.read()
    try:
        content = json.loads(content_json)
    except:
        content = {}


    base_dir = '/asset/' + dtable_uuid
    big_data_file_path = 'files/plugins/big-data-screen/%(page_id)s/' % ({
            'page_id': page_id,
        })
    image_file_path = 'files/plugins/big-data-screen/bg_images/'
    current_file_path = os.path.join(base_dir, big_data_file_path)
    current_image_path = os.path.join(base_dir, image_file_path)

    # 1. handle page_content
    page_content_dict = content.get('page_content')
    page_content_dict['page_id'] = page_id # update page_id
    tmp_page_json_path = os.path.join(tmp_extracted_path, '%s.json' % page_id)
    with open(tmp_page_json_path, 'wb') as f:
        f.write(json.dumps(page_content_dict).encode('utf-8'))

    path_id = seafile_api.get_dir_id_by_path(repo_id, current_file_path)
    if not path_id:
        seafile_api.mkdir_with_parents(repo_id, '/', current_file_path[1:], username)
    file_name = os.path.basename(tmp_page_json_path)
    dtable_file_id = seafile_api.get_file_id_by_path(
        repo_id, current_file_path + file_name)
    if dtable_file_id:
        seafile_api.del_file(repo_id, current_file_path, json.dumps([file_name]), '')
    seafile_api.post_file(repo_id, tmp_page_json_path, current_file_path, file_name, username)
    seafile_api.post_file(repo_id, new_content_poster_file_path, current_file_path, poster_file_name, username)

    # 2. handle images
    image_path_id = seafile_api.get_dir_id_by_path(repo_id, current_image_path)
    if not image_path_id:
        seafile_api.mkdir_with_parents(repo_id, '/', current_image_path[1:], username)
    for dirpath, _, filenames in os.walk(image_path):
        for image_name in filenames:
            tmp_image_path = os.path.join(dirpath, image_name)
            dtable_file_id = seafile_api.get_file_id_by_path(
                repo_id, current_image_path + image_name
            )
            if not dtable_file_id:
                seafile_api.post_file(repo_id, tmp_image_path, current_image_path, image_name, username)


def zip_big_data_screen_app(username, repo_id, dtable_uuid, app_uuid, app_id, tmp_file_path, db_session):
    from dtable_events.utils import uuid_str_to_36_chars, normalize_file_path, gen_file_get_url

    base_dir = '/asset/' + uuid_str_to_36_chars(dtable_uuid)

    big_data_poster_path = 'external-apps/big-data-screen/%(app_id)s/%(app_id)s.png' % ({
        'app_id': app_id
    })

    # asset_path = "%s/%s" % (base_dir, big_data_file_path)

    poster_asset_path = "%s/%s" % (base_dir, big_data_poster_path)

    sql = "SELECT app_config FROM dtable_external_apps WHERE id=:app_id"
    app = db_session.execute(text(sql), {'app_id': app_id}).fetchone()
    if not app:
        raise Exception('app: %s not found', app_id)
    try:
        app_config = json.loads(app.app_config)
    except Exception as e:
        raise Exception('app: %s app_config invalid', app_id)
    content = app_config.get('settings') or {}

    #  b. poster

    resp_poster = None
    poster_asset_id = seafile_api.get_file_id_by_path(repo_id, poster_asset_path)
    if poster_asset_id:
        poster_token = seafile_api.get_fileserver_access_token(
            repo_id, poster_asset_id, 'view', '', use_onetime=False
        )
        poster_name = os.path.basename(normalize_file_path(big_data_poster_path))
        url = gen_file_get_url(poster_token, poster_name)
        resp_poster = requests.get(url)

    # 2. get the image infos in big-data-screen
    page_bg_custom_image_list = content.get('page_bg_custom_image_list') or []
    page_images = []
    page_elements = content.get('page_elements') or {}
    element_map = page_elements.get('element_map') or {}
    for key, value in element_map.items():
        if value.get('element_type') == 'image':
            image_url = value.get('config',{}).get('imageUrl')
            if "?" not in image_url:  # when is there a ?
                page_images.append(image_url)

    content_json = {
        'page_content': content,
        'page_images': page_images
    }

    # 3. write json file to tmp_file_path , write images to tmp_file_path/images
    content_json_save_path = tmp_file_path.rstrip('/')
    image_save_path = os.path.join(content_json_save_path, 'images')
    with open("%s/content.json" % content_json_save_path, 'wb') as f:
        f.write(json.dumps(content_json).encode('utf-8'))
    if resp_poster:
        with open("%s/content.png" % content_json_save_path, 'wb') as f:
            f.write(resp_poster.content)

    for image_url in page_bg_custom_image_list:
        target_path = normalize_file_path(os.path.join(base_dir, f'external-apps/big-data-screen/{app_id}', image_url.strip('/')))
        asset_id = seafile_api.get_file_id_by_path(repo_id, target_path)
        if not asset_id:
            continue
        token = seafile_api.get_fileserver_access_token(
            repo_id, asset_id, 'download', username, use_onetime=False
        )
        image_name = os.path.basename(normalize_file_path(image_url))
        url = gen_file_get_url(token, image_name)

        resp = requests.get(url)
        with open('%s/%s' % (image_save_path, image_name), 'wb') as f:
            f.write(resp.content)

    for image_url in page_images:
        target_path = normalize_file_path(os.path.join(base_dir, f'external-apps/big-data-screen/{app_id}', image_url.strip('/')))
        asset_id = seafile_api.get_file_id_by_path(repo_id, target_path)
        if not asset_id:
            continue

        token = seafile_api.get_fileserver_access_token(
            repo_id, asset_id, 'download', username, use_onetime=False
        )
        image_name = os.path.basename(normalize_file_path(image_url))
        url = gen_file_get_url(token, image_name)

        resp = requests.get(url)
        with open('%s/%s' % (image_save_path, image_name), 'wb') as f:
            f.write(resp.content)


def post_big_data_screen_app_zip_file(username, repo_id, dtable_uuid, app_uuid, app_id, tmp_extracted_path, db_session):

    content_json_file_path = os.path.join(tmp_extracted_path, 'content.json')
    content_poster_file_path = os.path.join(tmp_extracted_path, 'content.png')
    new_content_poster_file_path = os.path.join(tmp_extracted_path, '%s.png' % app_id)
    poster_file_name = os.path.basename(new_content_poster_file_path)
    if os.path.isfile(content_poster_file_path):
        os.rename(content_poster_file_path, new_content_poster_file_path)

    image_path = os.path.join(tmp_extracted_path, 'images/')

    with open(content_json_file_path, 'r') as f:
        content_json = f.read()
    try:
        content = json.loads(content_json)
    except:
        content = {}
    app_name = content.get('app_name') or ''


    base_dir = '/asset/' + dtable_uuid

    big_data_file_path = 'external-apps/big-data-screen/%(app_id)s/' % {
        'app_id': app_id
    }

    image_file_path = '%(big_data_file_path)s/bg_images/' % {
        'big_data_file_path': big_data_file_path.strip('/')
    }
    current_file_path = os.path.join(base_dir, big_data_file_path)
    current_image_path = os.path.join(base_dir, image_file_path)

    # 1. handle page_content
    page_content_dict = content.get('page_content')
    page_content_dict['app_id'] = app_id # update app_id
    page_content_dict['app_uuid'] = app_uuid # update app_uuid

    sql = "SELECT app_config FROM dtable_external_apps WHERE id=:app_id"
    app_config = json.loads(db_session.execute(text(sql), {'app_id': app_id}).fetchone().app_config)
    app_config['settings'] = page_content_dict
    app_config['app_name'] = app_name
    sql = "UPDATE dtable_external_apps SET app_config=:app_config WHERE id=:app_id"
    db_session.execute(text(sql), {
        'app_config': json.dumps(app_config).encode('utf-8'),
        'app_id': app_id
    })
    db_session.commit()

    path_id = seafile_api.get_dir_id_by_path(repo_id, current_file_path)
    if not path_id:
        seafile_api.mkdir_with_parents(repo_id, '/', current_file_path[1:], username)

    if os.path.isfile(content_poster_file_path):
        seafile_api.post_file(repo_id, new_content_poster_file_path, current_file_path, poster_file_name, username)

    # 2. handle images
    image_path_id = seafile_api.get_dir_id_by_path(repo_id, current_image_path)
    if not image_path_id:
        seafile_api.mkdir_with_parents(repo_id, '/', current_image_path[1:], username)
    if os.path.isdir(image_path):
        for dirpath, _, filenames in os.walk(image_path):
            for image_name in filenames:
                tmp_image_path = os.path.join(dirpath, image_name)
                dtable_file_id = seafile_api.get_file_id_by_path(
                    repo_id, current_image_path + image_name
                )
                if not dtable_file_id:
                    seafile_api.post_file(repo_id, tmp_image_path, current_image_path, image_name, username)


def escape_sheet_name(text):
    # invalid_title_regex is from openpyxl
    invalid_title_regex = re.compile(r'[\\*?:/\[\]]')
    replacement = "-"
    return re.sub(invalid_title_regex, replacement, text)


def export_page_design_dir_to_path(repo_id, dtable_uuid, page_id, tmp_file_path, username=''):
    dir_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/page-design/{page_id}'
    dir_id = seafile_api.get_dir_id_by_path(repo_id, dir_path)
    if not dir_id:
        return
    fake_obj_id = {
        'obj_id': dir_id,
        'dir_name': page_id,
        'is_window': 0
    }
    token = seafile_api.get_fileserver_access_token(
        repo_id, json.dumps(fake_obj_id), 'download-dir', username, use_onetime=False
    )
    progress = {'zipped': 0, 'total': 1}
    while progress['zipped'] != progress['total']:
        time.sleep(0.5)   # sleep 0.5 second
        progress = json.loads(seafile_api.query_zip_progress(token))

    asset_url = gen_dir_zip_download_url(token)
    resp = requests.get(asset_url)
    with open(tmp_file_path, 'wb') as f:
        f.write(resp.content)


def download_page_design_file(repo_id, dtable_uuid, page_id, is_dir, username):
    if is_dir:
        file_name = f'{uuid_str_to_36_chars(dtable_uuid)}-{page_id}.zip'
        seafile_page_file_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/page-design/{file_name}'
    else:
        file_name = f'{uuid_str_to_36_chars(dtable_uuid)}-{page_id}.json'
        seafile_page_file_path = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/page-design/{file_name}'
    file_id = seafile_api.get_file_id_by_path(repo_id, seafile_page_file_path)
    token = seafile_api.get_fileserver_access_token(repo_id, file_id, 'download', username)
    download_url = gen_inner_file_get_url(token, os.path.basename(seafile_page_file_path))
    resp = requests.get(download_url)
    download_path = f'/tmp/dtable-io/page-design/{file_name}'
    with open(download_path, 'wb') as f:
        f.write(resp.content)
    seafile_api.del_file(repo_id, os.path.dirname(seafile_page_file_path), json.dumps([file_name]), username)
    if is_dir:
        download_dir_path = f"/tmp/dtable-io/page-design/{file_name.split('.')[0]}"
        with ZipFile(download_path, 'r') as zip_file:
            zip_file.extractall(download_dir_path)
        os.remove(download_path)
        dir_name = os.listdir(download_dir_path)[0]
        content_file_name = None
        for item in os.listdir(os.path.join(download_dir_path, dir_name)):
            if not content_file_name and item.endswith('.json'):
                content_file_name = item
            if not item.endswith('.json') and item not in ['static_image']:
                continue
            shutil.move(os.path.join(download_dir_path, dir_name, item), download_dir_path)
        shutil.rmtree(os.path.join(download_dir_path, dir_name))
        os.rename(os.path.join(download_dir_path, content_file_name), os.path.join(download_dir_path, f'{page_id}.json'))


def update_page_design_content_to_path(workspace_id, dtable_uuid, page_id, tmp_file_path, need_check_static):
    if not os.path.exists(tmp_file_path):
        return
    with open(tmp_file_path, 'r') as f:
        content = json.load(f)
    info = update_page_content(workspace_id, dtable_uuid, page_id, content, need_check_static)
    if info['is_changed']:
        with open(tmp_file_path, 'w') as f:
            json.dump(info['page_content'], f)


def upload_page_design(repo_id, dtable_uuid, page_id, tmp_page_path, is_dir, username=''):
    page_dir = f'/asset/{uuid_str_to_36_chars(dtable_uuid)}/page-design/{page_id}'
    page_dir_id = seafile_api.get_dir_id_by_path(repo_id, page_dir)
    if not page_dir_id:
        seafile_api.mkdir_with_parents(repo_id, os.path.dirname(page_dir), page_id, username)

    if is_dir:
        for root, _, files in os.walk(tmp_page_path):
            relative_path = root[len(tmp_page_path):].strip('/')
            parent_dir = os.path.join(page_dir, relative_path).strip('/')
            if not seafile_api.get_dir_id_by_path(repo_id, parent_dir):
                seafile_api.mkdir_with_parents(repo_id, os.path.dirname(parent_dir), os.path.basename(parent_dir), username)
            for file in files:
                seafile_api.post_file(repo_id, os.path.join(root, file), parent_dir, file, username)
    else:
        seafile_api.post_file(repo_id, tmp_page_path, page_dir, f'{page_id}.json', username)


def clear_tmp_dir(tmp_dir_path):
    if os.path.exists(tmp_dir_path):
        shutil.rmtree(tmp_dir_path)


def clear_tmp_file(tmp_file_path):
    if os.path.exists(tmp_file_path):
        os.remove(tmp_file_path)


def clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path):
    # delete tmp files/dirs
    clear_tmp_dir(tmp_file_path)
    clear_tmp_file(tmp_zip_path)


def save_file_by_path(file_path, content):
    with open(file_path, 'w') as f:
        f.write(content)


def get_table_names_by_dtable_server(username, dtable_uuid):
    from dtable_events.utils.dtable_server_api import DTableServerAPI
    dtable_server_api = DTableServerAPI(username, dtable_uuid, INNER_DTABLE_SERVER_URL)
    metadata = dtable_server_api.get_metadata()

    tables = metadata.get('tables', [])
    table_names = set()
    for t in tables:
        table_names.add(t.get('name'))

    return table_names


def get_non_duplicated_name(name, existed_names):
    if not existed_names or name not in existed_names:
        return name
    return get_no_duplicate_obj_name(name, existed_names)


def get_no_duplicate_obj_name(obj_name, exist_obj_names):
    def no_duplicate(obj_name):
        for exist_obj_name in exist_obj_names:
            if exist_obj_name == obj_name:
                return False
        return True

    def make_new_name(obj_name, i):
        return "%s (%d)" % (obj_name, i)

    if no_duplicate(obj_name):
        return obj_name
    else:
        i = 1
        while True:
            new_name = make_new_name(obj_name, i)
            if no_duplicate(new_name):
                return new_name
            else:
                i += 1


def filter_imported_tables(tables, included_tables):
    for table in tables:
        table_name = table.get('name')
        columns = table.get('columns')
        rows = table.get('rows')
        included_columns = included_tables.get(table_name)

        origin_columns_set = {column.get('name') for column in columns}
        excluded_columns_set = origin_columns_set - set(included_columns)
        if excluded_columns_set:
            new_columns = []
            for column in columns:
                column_name = column.get('name')
                if column_name in excluded_columns_set:
                    continue
                new_columns.append(column)
            if not new_columns:
                raise Exception('column can not be empty.')
            table['columns'] = new_columns

            new_rows = []
            for row in rows:
                for column_name in excluded_columns_set:
                    row.pop(column_name, None)
                # filter empty column
                if not row:
                    continue
                new_rows.append(row)
            table['rows'] = new_rows
    return tables


def image_column_offset_transfer(row_height, img_width, image_height):
    height_dict = {
        'default': 36,
        'double': 61,
        'triple': 76,
        'quadruple': 120
    }
    col_offset = height_dict.get(row_height, 36) * 7700 * (img_width / image_height)

    return col_offset


def image_row_offset_transfer(row_height):
    height_dict = {
        'default': 2,
        'double': 4,
        'triple': 6,
        'quadruple': 8
    }
    col_offset = height_dict.get(row_height, 2) * 7700

    return col_offset


def get_seadoc_download_link(repo_id, dtable_uuid, file_uuid, parent_path, filename,  is_inner=False):
    base_dir = gen_seadoc_base_dir(dtable_uuid, file_uuid)

    real_file_path = posixpath.join(base_dir, parent_path, filename)
    obj_id = seafile_api.get_file_id_by_path(repo_id, real_file_path)
    if not obj_id:
        return None
    token = seafile_api.get_fileserver_access_token(
        repo_id, obj_id, 'view', '', use_onetime=False)
    if not token:
        return None

    if is_inner:
        download_link = gen_inner_file_get_url(token, filename)
    else:
        download_link = gen_file_get_url(token, filename)

    return download_link


def gen_seadoc_base_dir(dtable_uuid, file_uuid):
    return posixpath.join('/asset', dtable_uuid, DOCUMENT_PLUGIN_FILE_RELATIVE_PATH, str(file_uuid))


def export_sdoc_prepare_images_folder(repo_id, images_dir_id, username, tmp_file_path):
    fake_obj_id = {
        'obj_id': images_dir_id,
        'dir_name': 'images',
        'is_windows': 0
    }

    token = seafile_api.get_fileserver_access_token(repo_id, json.dumps(fake_obj_id), 'download-dir', username, use_onetime=False)

    if not USE_GO_FILESERVER:
        progress = {'zipped': 0, 'total': 1}
        while progress['zipped'] != progress['total']:
            time.sleep(0.5)
            progress = json.loads(seafile_api.query_zip_progress(token))

    asset_url = '%s/zip/%s' % (get_inner_fileserver_root(), token)
    resp = requests.get(asset_url)
    file_obj = io.BytesIO(resp.content)
    if is_zipfile(file_obj):
        with ZipFile(file_obj) as zp:
            zp.extractall(tmp_file_path)


def batch_upload_sdoc_images(dtable_uuid, doc_uuid, repo_id, username, tmp_image_dir):
    from dtable_events.dtable_io import dtable_io_logger
    parent_path = gen_seadoc_image_parent_path(dtable_uuid, doc_uuid, repo_id, username)
    upload_link = get_seadoc_asset_upload_link(repo_id, parent_path, username)

    file_list = os.listdir(tmp_image_dir)

    for filename in file_list:
        file_path = posixpath.join(parent_path, filename)
        image_path = os.path.join(tmp_image_dir, filename)
        image_file = open(image_path, 'rb')
        files = {'file': image_file}
        data = {'parent_dir': parent_path, 'filename': filename, 'target_file': file_path}
        resp = requests.post(upload_link, files=files, data=data)
        if not resp.ok:
            dtable_io_logger.warning('upload sdoc image: %s failed: %s', filename, resp.text)


def gen_seadoc_image_parent_path(dtable_uuid, doc_uuid, repo_id, username):
    base_dir = gen_seadoc_base_dir(dtable_uuid, doc_uuid)

    parent_path = os.path.join(base_dir + SDOC_IMAGES_DIR)
    dir_id = seafile_api.get_dir_id_by_path(repo_id, parent_path)
    if not dir_id:
        seafile_api.mkdir_with_parents(repo_id, '/', parent_path[1:], username)
    return parent_path


def get_seadoc_asset_upload_link(repo_id, parent_path, username):
    obj_id = json.dumps({'parent_dir': parent_path})
    token = seafile_api.get_fileserver_access_token(repo_id, obj_id, 'upload-link', '', use_onetime=False)
    if not token:
        return None
    upload_link = gen_file_upload_url(token, 'upload-api', True)
    return upload_link


def gen_document_base_dir(dtable_uuid):
    return posixpath.join('/asset', dtable_uuid, DOCUMENT_PLUGIN_FILE_RELATIVE_PATH)


def get_documents_config(repo_id, dtable_uuid, username):
    document_plugin_dir = gen_document_base_dir(dtable_uuid)
    config_path = posixpath.join(document_plugin_dir, DOCUMENT_CONFIG_FILE_NAME)
    file_id = seafile_api.get_file_id_by_path(repo_id, config_path)
    if not file_id:
        return []
    token = seafile_api.get_fileserver_access_token(repo_id, file_id, 'download', username, use_onetime=True)
    url = gen_inner_file_get_url(token, DOCUMENT_CONFIG_FILE_NAME)
    resp = requests.get(url)
    documents_config = json.loads(resp.content)
    return documents_config


def save_documents_config(repo_id, dtable_uuid, username, documents_config):
    document_plugin_dir = gen_document_base_dir(dtable_uuid)
    obj_id = json.dumps({'parent_dir': document_plugin_dir})

    dir_id = seafile_api.get_dir_id_by_path(repo_id, document_plugin_dir)
    if not dir_id:
        seafile_api.mkdir_with_parents(repo_id, '/', document_plugin_dir, username)

    token = seafile_api.get_fileserver_access_token(repo_id, obj_id, 'upload-link', '', use_onetime=False)
    if not token:
        raise Exception('upload token invalid')

    upload_link = gen_file_upload_url(token, 'upload-api')
    upload_link = upload_link + '?replace=1'

    files = {
        'file': (DOCUMENT_CONFIG_FILE_NAME, documents_config)
    }
    data = {'parent_dir': document_plugin_dir, 'relative_path': '', 'replace': 1}
    resp = requests.post(upload_link, files=files, data=data)
    if not resp.ok:
        raise Exception(resp.text)
