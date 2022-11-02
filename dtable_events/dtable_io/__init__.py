import base64
import json
import os
import shutil
import time

import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from selenium import webdriver
from urllib import parse
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, SESSION_COOKIE_NAME, INNER_DTABLE_DB_URL
from dtable_events.dtable_io.big_data import import_excel_to_db, update_excel_to_db
from dtable_events.dtable_io.utils import setup_logger, \
    prepare_asset_file_folder, post_dtable_json, post_asset_files, \
    download_files_to_path, create_forms_from_src_dtable, copy_src_forms_to_json, \
    prepare_dtable_json_from_memory, update_page_design_static_image, \
    copy_src_auto_rules_to_json, create_auto_rules_from_src_dtable, sync_app_users_to_table, \
    copy_src_workflows_to_json, create_workflows_from_src_dtable, copy_src_external_app_to_json,\
    create_external_apps_from_src_dtable
from dtable_events.db import init_db_session_class
from dtable_events.dtable_io.excel import parse_excel_csv_to_json, import_excel_csv_by_dtable_server, \
    append_parsed_file_by_dtable_server, parse_append_excel_csv_upload_file_to_json, \
    import_excel_csv_add_table_by_dtable_server, update_parsed_file_by_dtable_server, \
    parse_update_excel_upload_excel_to_json, parse_update_csv_upload_csv_to_json, parse_and_import_excel_csv_to_dtable, \
    parse_and_import_excel_csv_to_table, parse_and_update_file_to_table, parse_and_append_excel_csv_to_table
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.statistics.db import save_email_sending_records, batch_save_email_sending_records
from dtable_events.data_sync.data_sync_utils import run_sync_emails
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.dtable_server_api import DTableServerAPI

dtable_io_logger = setup_logger('dtable_events_io.log')
dtable_message_logger = setup_logger('dtable_events_message.log')
dtable_data_sync_logger = setup_logger('dtable_events_data_sync.log')
dtable_plugin_email_logger = setup_logger('dtable_events_plugin_email.log')


def clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path):
    # delete tmp files/dirs
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)
    if os.path.exists(tmp_zip_path):
        os.remove(tmp_zip_path)

def get_dtable_export_content(username, repo_id, workspace_id, dtable_uuid, asset_dir_id, config):
    """
    1. prepare file content at /tmp/dtable-io/<dtable_id>/dtable_asset/...
    2. make zip file
    3. return zip file's content
    """
    dtable_io_logger.info('Start prepare /tmp/dtable-io/{}/zip_file.zip for export DTable.'.format(dtable_uuid))

    tmp_file_path = os.path.join('/tmp/dtable-io', dtable_uuid,
                                 'dtable_asset/')  # used to store asset files and json from file_server
    tmp_zip_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'zip_file') + '.zip'  # zip path of zipped xxx.dtable

    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        raise Exception('create db session failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Clear tmp dirs and files before prepare.')
    clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path)
    os.makedirs(tmp_file_path, exist_ok=True)
    # import here to avoid circular dependency

    # 1. create 'content.json' from 'xxx.dtable'
    dtable_io_logger.info('Create content.json file.')
    try:
        prepare_dtable_json_from_memory(workspace_id, dtable_uuid, username)
    except Exception as e:
        dtable_io_logger.error('prepare dtable json failed. ERROR: {}'.format(e))
        raise Exception('prepare dtable json failed. ERROR: {}'.format(e))

    # 2. get asset file folder, asset could be empty
    if asset_dir_id:
        dtable_io_logger.info('Create asset dir.')
        try:
            prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id)
        except Exception as e:
            dtable_io_logger.error('create asset folder failed. ERROR: {}'.format(e))
            raise Exception('create asset folder failed. ERROR: {}'.format(e))

    # 3. copy forms
    try:
        copy_src_forms_to_json(dtable_uuid, tmp_file_path, db_session)
    except Exception as e:
        dtable_io_logger.error('copy forms failed. ERROR: {}'.format(e))
        raise Exception('copy forms failed. ERROR: {}'.format(e))
    finally:
        if db_session:
            db_session.close()

    # 4. copy automation rules
    try:
        copy_src_auto_rules_to_json(dtable_uuid, tmp_file_path, db_session)
    except Exception as e:
        dtable_io_logger.error('copy automation rules failed. ERROR: {}'.format(e))
        raise Exception('copy automation rules failed. ERROR: {}'.format(e))
    finally:
        if db_session:
            db_session.close()

    # 5. copy workflows
    try:
        copy_src_workflows_to_json(dtable_uuid, tmp_file_path, db_session)
    except Exception as e:
        dtable_io_logger.error('copy workflows failed. ERROR: {}'.format(e))
        raise Exception('copy workflows failed. ERROR: {}'.format(e))
    finally:
        if db_session:
            db_session.close()

    # 5. copy external app
    try:
        copy_src_external_app_to_json(dtable_uuid, tmp_file_path, db_session)
    except Exception as e:
        dtable_io_logger.error('copy external apps failed. ERROR: {}'.format(e))
        raise Exception('copy external apps failed. ERROR: {}'.format(e))
    finally:
        if db_session:
            db_session.close()

    """
    /tmp/dtable-io/<dtable_uuid>/dtable_asset/
                                    |- asset/
                                    |- content.json

    we zip /tmp/dtable-io/<dtable_uuid>/dtable_asset/ to /tmp/dtable-io/<dtable_id>/zip_file.zip and download it
    notice than make_archive will auto add .zip suffix to /tmp/dtable-io/<dtable_id>/zip_file
    """
    dtable_io_logger.info('Make zip file for download...')
    try:
        shutil.make_archive('/tmp/dtable-io/' + dtable_uuid + '/zip_file', "zip", root_dir=tmp_file_path)
    except Exception as e:
        dtable_io_logger.error('make zip failed. ERROR: {}'.format(e))
        raise Exception('make zip failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Create /tmp/dtable-io/{}/zip_file.zip success!'.format(dtable_uuid))
    # we remove '/tmp/dtable-io/<dtable_uuid>' in dtable web api


def post_dtable_import_files(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage,
                             can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id, config):
    """
    post files at /tmp/<dtable_uuid>/dtable_zip_extracted/ to file server
    unzip django uploaded tmp file is suppose to be done in dtable-web api.
    """
    dtable_io_logger.info('Start import DTable: {}.'.format(dtable_uuid))

    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Prepare dtable json file and post it at file server.')
    try:
        dtable_content = post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage)
    except Exception as e:
        dtable_io_logger.error('post dtable json failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Post asset files in tmp path to file server.')
    try:
        post_asset_files(repo_id, dtable_uuid, username)
    except Exception as e:
        dtable_io_logger.error('post asset files failed. ERROR: {}'.format(e))

    dtable_io_logger.info('create forms from src dtable.')
    try:
        create_forms_from_src_dtable(workspace_id, dtable_uuid, db_session)
    except Exception as e:
        dtable_io_logger.error('create forms failed. ERROR: {}'.format(e))
    finally:
        if db_session:
            db_session.close()

    if can_use_automation_rules:
        dtable_io_logger.info('create auto rules from src dtable.')
        try:
            create_auto_rules_from_src_dtable(username, workspace_id, repo_id, owner, org_id, dtable_uuid, db_session)
        except Exception as e:
            dtable_io_logger.error('create auto rules failed. ERROR: {}'.format(e))
        finally:
            if db_session:
                db_session.close()

    if can_use_workflows:
        dtable_io_logger.info('create workflows from src dtable.')
        try:
            create_workflows_from_src_dtable(username, workspace_id, repo_id, dtable_uuid, owner, org_id, db_session)
        except Exception as e:
            dtable_io_logger.error('create workflows failed. ERROR: {}'.format(e))
        finally:
            if db_session:
                db_session.close()

    if can_use_external_apps:
        dtable_io_logger.info('create external apps from src dtable.')
        try:
            create_external_apps_from_src_dtable(username, dtable_uuid, db_session, org_id)
        except Exception as e:
            dtable_io_logger.error('create external apps failed. ERROR: {}'.format(e))
        finally:
            if db_session:
                db_session.close()

    try:
        if dtable_content:
            plugin_settings = dtable_content.get('plugin_settings', {})
            page_design_settings = plugin_settings.get('page-design', [])
            page_design_content_json_tmp_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'page-design')
            # handle different url in settings.py
            dtable_web_service_url = DTABLE_WEB_SERVICE_URL
            file_server_port = task_manager.conf['file_server_port']
            update_page_design_static_image(page_design_settings, repo_id, workspace_id, dtable_uuid, page_design_content_json_tmp_path, dtable_web_service_url, file_server_port, username)
    except Exception as e:
        dtable_io_logger.error('update page design static image failed. ERROR: {}'.format(e))

    # remove extracted tmp file
    dtable_io_logger.info('Remove extracted tmp file.')
    try:
        shutil.rmtree(os.path.join('/tmp/dtable-io', dtable_uuid))
    except Exception as e:
        dtable_io_logger.error('rm extracted tmp file failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Import DTable: {} success!'.format(dtable_uuid))

def get_dtable_export_asset_files(username, repo_id, dtable_uuid, files, task_id, files_map=None):
    """
    export asset files from dtable
    """
    files = [f.strip().strip('/') for f in files]
    tmp_file_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'asset-files', 
                                 str(task_id))           # used to store files
    tmp_zip_path  = os.path.join('/tmp/dtable-io', dtable_uuid, 'asset-files',
                                 str(task_id)) + '.zip'  # zip those files

    clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path)
    os.makedirs(tmp_file_path, exist_ok=True)

    try:
        # 1. download files to tmp_file_path
        download_files_to_path(username, repo_id, dtable_uuid, files, tmp_file_path, files_map)
        # 2. zip those files to tmp_zip_path
        shutil.make_archive(tmp_zip_path.split('.')[0], 'zip', root_dir=tmp_file_path)
    except Exception as e:
        dtable_io_logger.error('export asset files from dtable failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('export files from dtable: %s success!', dtable_uuid)

def parse_excel_csv(username, repo_id, workspace_id, dtable_name, file_type, custom, config):
    """
    parse excel or csv to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse excel or csv: %s.%s.' % (dtable_name, file_type))
    try:
        parse_excel_csv_to_json(repo_id, dtable_name, file_type, custom)
    except Exception as e:
        dtable_io_logger.exception('parse excel or csv failed. ERROR: {}'.format(e))
        if str(e.args[0]) == 'Excel format error':
            raise Exception('Excel format error')
    else:
        dtable_io_logger.info('parse excel %s.xlsx success!' % dtable_name)

def import_excel_csv(username, repo_id, workspace_id, dtable_uuid, dtable_name, lang, config):
    """
    upload excel or csv json file to dtable-server
    """
    dtable_io_logger.info('Start import excel or csv: {}.'.format(dtable_uuid))
    try:
        import_excel_csv_by_dtable_server(username, repo_id, dtable_uuid, dtable_name, lang)
    except Exception as e:
        dtable_io_logger.error('import excel or csv failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('import excel or csv %s success!' % dtable_name)

def import_excel_csv_add_table(username, repo_id, workspace_id, dtable_uuid, dtable_name, lang, config):
    """
    add table, upload excel or csv json file to dtable-server
    """
    dtable_io_logger.info('Start import excel or csv add table: {}.'.format(dtable_uuid))
    try:
        import_excel_csv_add_table_by_dtable_server(username, repo_id, dtable_uuid, dtable_name, lang)
    except Exception as e:
        dtable_io_logger.error('import excel or csv add table failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('import excel or csv %s add table success!' % dtable_name)


def append_excel_csv_append_parsed_file(username, repo_id, dtable_uuid, file_name, table_name):
    """
    upload excel or csv json file to dtable-server
    """
    dtable_io_logger.info('Start import excel or csv: {}.'.format(dtable_uuid))
    try:
        append_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name)
    except Exception as e:
        dtable_io_logger.error('append excel or csv failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('append excel or csv %s success!' % file_name)

def append_excel_csv_upload_file(username, repo_id, file_name, dtable_uuid, table_name, file_type):
    """
    parse excel or csv to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse append excel or csv: %s.%s' % (file_name, file_type))
    try:
        parse_append_excel_csv_upload_file_to_json(repo_id, file_name, username, dtable_uuid, table_name, file_type)
    except Exception as e:
        dtable_io_logger.exception('parse append excel or csv failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('parse append excel or csv %s.%s success!' % (file_name, file_type))

def update_excel_csv_update_parsed_file(username, repo_id, dtable_uuid, file_name, table_name, selected_columns):
    """
    upload excel/csv json file to dtable-server
    """
    dtable_io_logger.info('Start import file: {}.'.format(dtable_uuid))
    try:
        update_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name, selected_columns)
    except Exception as e:
        dtable_io_logger.error('update excel,csv failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('update excel,csv %s success!' % file_name)

def update_excel_upload_excel(username, repo_id, file_name, dtable_uuid, table_name):
    """
    parse excel to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse update excel: %s.xlsx.' % file_name)
    try:
        parse_update_excel_upload_excel_to_json(repo_id, file_name, username, dtable_uuid, table_name)
    except Exception as e:
        dtable_io_logger.exception('parse update excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('parse update excel %s.xlsx success!' % file_name)

def update_csv_upload_csv(username, repo_id, file_name, dtable_uuid, table_name):
    """
    parse csv to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse update csv: %s.csv.' % file_name)
    try:
        parse_update_csv_upload_csv_to_json(repo_id, file_name, username, dtable_uuid, table_name)
    except Exception as e:
        dtable_io_logger.exception('parse update csv failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('parse update csv %s.csv success!' % file_name)


def import_excel_csv_to_dtable(username, repo_id, workspace_id, dtable_name, dtable_uuid, file_type, lang):
    """
    parse excel csv to json, then import excel csv to dtable
    """
    dtable_io_logger.info('Start import excel or csv: %s.%s to dtable.' % (dtable_name, file_type))
    try:
        parse_and_import_excel_csv_to_dtable(repo_id, dtable_name, dtable_uuid, username, file_type, lang)
    except Exception as e:
        dtable_io_logger.exception('import excel or csv to dtable failed. ERROR: {}'.format(e))
        if str(e.args[0]) == 'Excel format error':
            raise Exception('Excel format error')
    else:
        dtable_io_logger.info('import excel or csv %s.%s to dtable success!' % (dtable_name, file_type))


def import_excel_csv_to_table(username, repo_id, workspace_id, file_name, dtable_uuid, file_type, lang):
    """
    parse excel or csv to json, then import excel or csv to table
    """
    dtable_io_logger.info('Start import excel or csv: %s.%s to table.' % (file_name, file_type))
    try:
        parse_and_import_excel_csv_to_table(repo_id, file_name, dtable_uuid, username, file_type, lang)
    except Exception as e:
        dtable_io_logger.exception('import excel or csv to table failed. ERROR: {}'.format(e))
        if str(e.args[0]) == 'Excel format error':
            raise Exception('Excel format error')
    else:
        dtable_io_logger.info('import excel or csv %s.%s to table success!' % (file_name, file_type))


def update_table_via_excel_csv(username, repo_id, file_name, dtable_uuid, table_name, selected_columns, file_type):
    """
    update excel/csv file to table
    """
    dtable_io_logger.info('Start update file: %s.%s to table.' % (file_name, file_type))
    try:
        parse_and_update_file_to_table(repo_id, file_name, username, dtable_uuid, table_name, selected_columns, file_type)
    except Exception as e:
        dtable_io_logger.exception('update file update to table failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('update file %s.%s update to table success!' % (file_name, file_type))


def append_excel_csv_to_table(username, repo_id, file_name, dtable_uuid, table_name, file_type):
    """
    parse excel or csv to json, then append excel or csv to table
    """
    dtable_io_logger.info('Start append excel or csv: %s.%s to table.' % (file_name, file_type))
    try:
        parse_and_append_excel_csv_to_table(username, repo_id, file_name, dtable_uuid, table_name, file_type)
    except Exception as e:
        dtable_io_logger.exception('append excel or csv to table failed. ERROR: {}'.format(e))
        if str(e.args[0]) == 'Excel format error':
            raise Exception('Excel format error')
    else:
        dtable_io_logger.info('append excel or csv %s.%s to table success!' % (file_name, file_type))


def _get_upload_link_to_seafile(seafile_server_url, access_token, parent_dir="/"):
    upload_link_api_url = "%s%s" % (seafile_server_url.rstrip('/'),  '/api/v2.1/via-repo-token/upload-link/')
    headers = {
        'authorization': 'Token ' + access_token
    }
    params = {
        'path': parent_dir
    }
    response = requests.get(upload_link_api_url, headers=headers, params=params)
    return response.json()

def _upload_to_seafile(seafile_server_url, access_token, files, parent_dir="/", relative_path="", replace=None):
    upload_url = _get_upload_link_to_seafile(seafile_server_url, access_token, parent_dir)
    files_tuple_list = [('file', open(file, 'rb')) for file in files]
    files = files_tuple_list + [('parent_dir', parent_dir), ('relative_path', relative_path), ('replace', replace)]
    response = requests.post(upload_url, files=files)
    return response

def get_dtable_transfer_asset_files(username, repo_id, dtable_uuid, files, task_id, files_map, parent_dir, relative_path, replace, repo_api_token, seafile_server_url):
    tmp_file_path = os.path.join('/tmp/dtable-io/', dtable_uuid, 'transfer-files', str(task_id))
    os.makedirs(tmp_file_path, exist_ok=True)

    # download files to local
    local_file_list = download_files_to_path(username, repo_id, dtable_uuid, files, tmp_file_path, files_map)

    # upload files from local to seafile
    try:
        _upload_to_seafile(seafile_server_url, repo_api_token, local_file_list, parent_dir, relative_path, replace)
    except Exception as e:
        dtable_io_logger.error('transfer asset files from dtable failed. ERROR: {}'.format(e))

    # delete local files
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)

def send_wechat_msg(webhook_url, msg, msg_type="text"):
    if msg_type == "markdown":
        msg_format = {"msgtype": "markdown","markdown":{"content":msg}}
    else:
        msg_format = {"msgtype": "text", "text": {"content": msg}}
    result = {}
    try:
        requests.post(webhook_url, json=msg_format, headers={"Content-Type": "application/json"})
    except Exception as e:
        dtable_message_logger.error('Wechat sending failed. ERROR: {}'.format(e))
        result['err_msg'] = 'Webhook URL invalid'
    else:
        dtable_message_logger.info('Wechat sending success!')
    return result

def send_dingtalk_msg(webhook_url, msg, msg_type="text", msg_title=None):
    result = {}
    if msg_type == "markdown":
        if not msg_title:
            result['err_msg'] = 'msg_title invalid'
            dtable_message_logger.error('Dingtalk sending failed. ERROR: msg_title invalid')
            return result
        msg_format = {"msgtype": "markdown", "markdown": {"text": msg, "title": msg_title}}
    else:
        msg_format = {"msgtype": "text", "text": {"content": msg}}

    try:
        requests.post(webhook_url, json=msg_format, headers={"Content-Type": "application/json"})
    except Exception as e:
        dtable_message_logger.error('Dingtalk sending failed. ERROR: {}'.format(e))
        result['err_msg'] = 'Dingtalk URL invalid'
    else:
        dtable_message_logger.info('Dingtalk sending success!')
    return result


def send_email_msg(auth_info, send_info, username, config=None, db_session=None):
    # auth info
    email_host = auth_info.get('email_host')
    email_port = int(auth_info.get('email_port'))
    host_user = auth_info.get('host_user')
    password = auth_info.get('password')

    # send info
    msg = send_info.get('message', '')
    html_msg = send_info.get('html_message', '')
    send_to = send_info.get('send_to', [])
    subject = send_info.get('subject', '')
    source = send_info.get('source', '')
    copy_to = send_info.get('copy_to', [])
    reply_to = send_info.get('reply_to', '')
    file_download_urls = send_info.get('file_download_urls', None)
    message_id = send_info.get('message_id', '')
    in_reply_to = send_info.get('in_reply_to', '')

    send_to = [formataddr(parseaddr(to)) for to in send_to]
    copy_to = [formataddr(parseaddr(to)) for to in copy_to]
    if source:
        source = formataddr(parseaddr(source))

    result = {}
    if not msg and not html_msg:
        result['err_msg'] = 'Email message invalid'
        return result

    msg_obj = MIMEMultipart()
    msg_obj['Subject'] = subject
    msg_obj['From'] = source or host_user
    msg_obj['To'] = ",".join(send_to)
    msg_obj['Cc'] = ",".join(copy_to)
    msg_obj['Reply-to'] = reply_to

    if message_id:
        msg_obj['Message-ID'] = message_id

    if in_reply_to:
        msg_obj['In-Reply-To'] = in_reply_to

    if msg:
        plain_content_body = MIMEText(msg)
        msg_obj.attach(plain_content_body)

    if html_msg:
        html_content_body = MIMEText(html_msg, 'html')
        msg_obj.attach(html_content_body)

    if file_download_urls:
        for file_name, file_url in file_download_urls.items():
            response = requests.get(file_url)
            attach_file = MIMEText(response.content, 'base64', 'utf-8')
            attach_file["Content-Type"] = 'application/octet-stream'
            attach_file["Content-Disposition"] = 'attachment;filename*=UTF-8\'\'' + parse.quote(file_name)
            msg_obj.attach(attach_file)

    try:
        smtp = smtplib.SMTP(email_host, int(email_port), timeout=30)
    except Exception as e:
        dtable_message_logger.warning(
            'Email server configured failed. host: %s, port: %s, error: %s' % (email_host, email_port, e))
        result['err_msg'] = 'Email server host or port invalid'
        return result
    success = False

    try:
        smtp.starttls()
        smtp.login(host_user, password)
        recevers = copy_to and send_to + copy_to or send_to
        smtp.sendmail(host_user, recevers, msg_obj.as_string())
        success = True
    except Exception as e:
        dtable_message_logger.warning(
            'Email sending failed. email: %s, error: %s' % (host_user, e))
        result['err_msg'] = 'Email server username or password invalid'
    else:
        dtable_message_logger.info('Email sending success!')
    finally:
        smtp.quit()

    session = db_session or init_db_session_class(config)()
    try:
        save_email_sending_records(session, username, email_host, success)
    except Exception as e:
        dtable_message_logger.error(
            'Email sending log record error: %s' % e)
    finally:
        session.close()
    return result


def batch_send_email_msg(auth_info, send_info_list, username, config=None, db_session=None):
    """
    for personal user of email, this function only support sending 10 emails per time
    """
    # auth info
    email_host = auth_info.get('email_host')
    email_port = int(auth_info.get('email_port'))
    host_user = auth_info.get('host_user')
    password = auth_info.get('password')

    try:
        smtp = smtplib.SMTP(email_host, int(email_port), timeout=30)
    except Exception as e:
        dtable_message_logger.warning(
            'Email server configured failed. host: %s, port: %s, error: %s' % (email_host, email_port, e))
        return

    try:
        smtp.starttls()
        smtp.login(host_user, password)
    except Exception as e:
        dtable_message_logger.warning(
            'Login smtp failed, host user: %s, error: %s' % (host_user, e))
        return

    send_state_list = []
    for send_info in send_info_list:
        success = False
        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        send_to = send_info.get('send_to', [])
        subject = send_info.get('subject', '')
        source = send_info.get('source', '')
        copy_to = send_info.get('copy_to', [])
        reply_to = send_info.get('reply_to', '')
        file_download_urls = send_info.get('file_download_urls', None)

        if not msg and not html_msg:
            dtable_message_logger.warning('Email message invalid')
            continue

        msg_obj = MIMEMultipart()
        msg_obj['Subject'] = subject
        msg_obj['From'] = source or host_user
        msg_obj['To'] = ",".join(send_to)
        msg_obj['Cc'] = copy_to and ",".join(copy_to) or ""
        msg_obj['Reply-to'] = reply_to

        if msg:
            plain_content_body = MIMEText(msg)
            msg_obj.attach(plain_content_body)

        if html_msg:
            html_content_body = MIMEText(html_msg, 'html')
            msg_obj.attach(html_content_body)

        if file_download_urls:
            for file_name, file_url in file_download_urls.items():
                response = requests.get(file_url)
                attach_file = MIMEText(response.content, 'base64', 'utf-8')
                attach_file["Content-Type"] = 'application/octet-stream'
                attach_file["Content-Disposition"] = 'attachment;filename*=UTF-8\'\'' + parse.quote(file_name)
                msg_obj.attach(attach_file)

        try:
            recevers = copy_to and send_to + copy_to or send_to
            smtp.sendmail(host_user, recevers, msg_obj.as_string())
            success = True
        except Exception as e:
            dtable_message_logger.warning('Email sending failed. email: %s, error: %s' % (host_user, e))
        else:
            dtable_message_logger.info('Email sending success!')
        send_state_list.append(success)
        time.sleep(0.5)

    smtp.quit()

    session = db_session or init_db_session_class(config)()
    try:
        batch_save_email_sending_records(session, username, email_host, send_state_list)
    except Exception as e:
        dtable_message_logger.error('Batch save email sending log error: %s' % e)
    finally:
        session.close()


def convert_page_to_pdf(dtable_uuid, page_id, row_id, access_token, session_id):
    if not row_id:
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/' % (dtable_uuid, page_id)
    if row_id:
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/dtable/%s/page-design/%s/row/%s/' % (dtable_uuid, page_id, row_id)
    url += '?access-token=%s&need_convert=%s' % (access_token, 0)
    target_dir = '/tmp/dtable-io/convert-page-to-pdf'
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)
    target_path = os.path.join(target_dir, '%s_%s_%s.pdf' % (dtable_uuid, page_id, row_id))

    webdriver_options = Options()
    driver = None

    webdriver_options.add_argument('--no-sandbox')
    webdriver_options.add_argument('--headless')
    webdriver_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome('/usr/local/bin/chromedriver', options=webdriver_options)

    driver.get(DTABLE_WEB_SERVICE_URL)
    cookies = [{
        'name': SESSION_COOKIE_NAME,
        'value': session_id
    }]
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.get(url)

    def check_images_and_networks(driver, frequency=0.5):
        """
        make sure all images complete
        make sure no new connections in 0.5s.
        TODO: Unreliable and need to be continuously updated.
        """
        images_done = driver.execute_script('''
            let p = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
            let entries = p.getEntries();
            let images = Array.from(document.images).filter(image => image.src.indexOf('/asset/') !== -1);
            if (images.length === 0) return true;
            return images.filter(image => image.complete).length == images.length;
        ''')
        if not images_done:
            return False

        entries_count = None
        while True:
            now_entries_count = driver.execute_script('''
                let p = window.performance || window.mozPerformance || window.msPerformance || window.webkitPerformance || {};
                return p.getEntries().length;
            ''')
            if entries_count is None:
                entries_count = now_entries_count
                time.sleep(frequency)
                continue
            else:
                if now_entries_count == entries_count and \
                    driver.execute_script("return document.readyState === 'complete'"):
                    return True
                break
        return False

    awaitReactRender = 60
    sleepTime = 2
    if not row_id:
        awaitReactRender = 180
        sleepTime = 6

    try:
        # make sure react is rendered, timeout awaitReactRender, rendering is not completed within 3 minutes, and rendering performance needs to be improved
        WebDriverWait(driver, awaitReactRender).until(lambda driver: driver.find_element_by_id('page-design-render-complete') is not None, message='wait react timeout')
        # make sure images from asset are rendered, timeout 120s
        WebDriverWait(driver, 120, poll_frequency=1).until(lambda driver: check_images_and_networks(driver), message='wait images and networks timeout')
        time.sleep(sleepTime) # wait for all rendering
    except Exception as e:
        dtable_io_logger.warning('wait for page design error: %s', e)
    finally:
        calculated_print_options = {
            'landscape': False,
            'displayHeaderFooter': False,
            'printBackground': True,
            'preferCSSPageSize': True,
        }
        
        resource = "/session/%s/chromium/send_command_and_get_result" % driver.session_id
        url = driver.command_executor._url + resource
        body = json.dumps({'cmd': 'Page.printToPDF', 'params': calculated_print_options})

        try:
            response = driver.command_executor._request('POST', url, body)
            if not response:
                dtable_io_logger.error('execute printToPDF error no response')
            v = response.get('value')['data']
            with open(target_path, 'wb') as f:
                f.write(base64.b64decode(v))
            dtable_io_logger.info('convert page to pdf success!')
        except Exception as e:
            dtable_io_logger.error('execute printToPDF error: {}'.format(e))

        driver.quit()


def parse_view_rows(response_rows, head_list, summary_col_info, cols_without_hidden):
    """
    return data_list for rows data, grouped_row_num_map like {1: 0, 2: 1, 5: 1, 8: 0, 9: 1} means position of summary row
    """
    from dtable_events.dtable_io.excel import parse_grouped_rows
    if response_rows and ('rows' in response_rows[0] or 'subgroups' in response_rows[0]):
        first_col_name = head_list[0][0]
        head_name_to_head = {head[0]: head for head in head_list}
        result_rows, grouped_row_num_map = parse_grouped_rows(response_rows, first_col_name, summary_col_info, head_name_to_head)
    else:
        result_rows, grouped_row_num_map = response_rows, {}

    data_list = []
    for row_from_server in result_rows:
        row = []
        for col in cols_without_hidden:
            cell_data = row_from_server.get(col['name'], '')
            row.append(cell_data)
        data_list.append(row)
    return data_list, grouped_row_num_map


def convert_view_to_execl(dtable_uuid, table_id, view_id, username, id_in_org, permission, name):
    from dtable_events.dtable_io.utils import get_metadata_from_dtable_server, get_view_rows_from_dtable_server, \
        convert_db_rows
    from dtable_events.dtable_io.excel import write_xls_with_type
    from dtable_events.dtable_io.utils import get_related_nicknames_from_dtable
    from dtable_events.app.config import ARCHIVE_VIEW_EXPORT_ROW_LIMIT
    import openpyxl

    target_dir = '/tmp/dtable-io/export-view-to-excel/' + dtable_uuid
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    try:
        nicknames = get_related_nicknames_from_dtable(dtable_uuid, username, permission)
    except Exception as e:
        dtable_io_logger.error('get nicknames. ERROR: {}'.format(e))
        return
    email2nickname = {nickname['email']: nickname['name'] for nickname in nicknames}

    try:
        metadata = get_metadata_from_dtable_server(dtable_uuid, username, permission)
    except Exception as e:
        dtable_io_logger.error('get metadata. ERROR: {}'.format(e))
        return

    target_table = {}
    target_view = {}
    for table in metadata.get('tables', []):
        if table.get('_id', '') == table_id:
            target_table = table
            break

    if not target_table:
        dtable_io_logger.error('Table %s not found.' % table_id)
        return

    for view in target_table.get('views', []):
        if view.get('_id', '') == view_id:
            target_view = view
            break
    if not target_view:
        dtable_io_logger.error('View %s not found.' % view_id)
        return

    table_name = target_table.get('name', '')
    view_name = target_view.get('name', '')
    view_type = target_view.get('type', '')
    if view_type == 'archive':
        is_archive = True
    else:
        is_archive = False
    cols = target_table.get('columns', [])
    hidden_cols_key = target_view.get('hidden_columns', [])
    summary_configs = target_table.get('summary_configs', {})
    cols_without_hidden = []
    summary_col_info = {}
    head_list = []
    for col in cols:
        if col.get('key', '') not in hidden_cols_key:
            cols_without_hidden.append(col)
            head_list.append((col.get('name', ''), col.get('type', ''), col.get('data', '')))
        if summary_configs.get(col.get('key')):
            summary_col_info.update({col.get('name'): summary_configs.get(col.get('key'))})

    sheet_name = table_name + ('_' + view_name if view_name else '')
    excel_name = name + '_' + table_name + ('_' + view_name if view_name else '') + '.xlsx'
    target_path = os.path.join(target_dir, excel_name)
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(sheet_name)
    if is_archive:
        step = 10000
        archive_view_export_row_limit = int(ARCHIVE_VIEW_EXPORT_ROW_LIMIT)
        archive_metadata = []

        for i in range(0, archive_view_export_row_limit, step):
            limit = step if (archive_view_export_row_limit - i > step) else (archive_view_export_row_limit - i)

            res_json = get_view_rows_from_dtable_server(dtable_uuid, table_id, view_id, username, id_in_org, permission,
                                                        table_name, view_name, start=i, limit=limit)
            rows = res_json.get('rows', [])

            if i == 0:
                archive_metadata = res_json.get('metadata')

            response_rows = convert_db_rows(archive_metadata, rows)
            data_list, grouped_row_num_map = parse_view_rows(response_rows, head_list, summary_col_info, cols_without_hidden)

            row_num = i

            try:
                write_xls_with_type(head_list, data_list, grouped_row_num_map, email2nickname, ws, row_num)
            except Exception as e:
                dtable_io_logger.exception(e)
                dtable_io_logger.error('head_list = {}\n{}'.format(head_list, e))
                return

            if len(rows) < step:
                break
        wb.save(target_path)
    else:
        res_json = get_view_rows_from_dtable_server(dtable_uuid, table_id, view_id, username, id_in_org, permission,
                                                    table_name, view_name)
        response_rows = res_json.get('rows', [])

        data_list, grouped_row_num_map = parse_view_rows(response_rows, head_list, summary_col_info, cols_without_hidden)

        try:
            write_xls_with_type(head_list, data_list, grouped_row_num_map, email2nickname, ws, 0)
        except Exception as e:
            dtable_io_logger.error('head_list = {}\n{}'.format(head_list, e))
            return
        target_path = os.path.join(target_dir, excel_name)
        wb.save(target_path)


def convert_table_to_execl(dtable_uuid, table_id, username, permission, name):
    from dtable_events.dtable_io.utils import get_metadata_from_dtable_server, get_rows_from_dtable_server, \
        convert_db_rows
    from dtable_events.dtable_io.excel import write_xls_with_type
    from dtable_events.dtable_io.utils import get_related_nicknames_from_dtable
    import openpyxl

    target_dir = '/tmp/dtable-io/export-table-to-excel/' + dtable_uuid
    if not os.path.isdir(target_dir):
        os.makedirs(target_dir)

    try:
        nicknames = get_related_nicknames_from_dtable(dtable_uuid, username, permission)
    except Exception as e:
        dtable_io_logger.error('get nicknames. ERROR: {}'.format(e))
        return
    email2nickname = {nickname['email']: nickname['name'] for nickname in nicknames}

    try:
        metadata = get_metadata_from_dtable_server(dtable_uuid, username, permission)
    except Exception as e:
        dtable_io_logger.error('get metadata. ERROR: {}'.format(e))
        return

    target_table = {}
    for table in metadata.get('tables', []):
        if table.get('_id', '') == table_id:
            target_table = table
            break

    if not target_table:
        dtable_io_logger.error('Table %s not found.' % table_id)
        return

    table_name = target_table.get('name', '')
    cols = target_table.get('columns', [])
    head_list = []
    for col in cols:
        head_list.append((col.get('name', ''), col.get('type', ''), col.get('data', '')))

    result_rows = get_rows_from_dtable_server(username, dtable_uuid, table_name)

    data_list = []
    for row_from_server in result_rows:
        row = []
        for col in cols:
            cell_data = row_from_server.get(col['name'], '')
            row.append(cell_data)
        data_list.append(row)

    sheet_name = table_name
    excel_name = name + '_' + table_name + '.xlsx'
    target_path = os.path.join(target_dir, excel_name)
    wb = openpyxl.Workbook(write_only=True)
    ws = wb.create_sheet(sheet_name)
    try:
        write_xls_with_type(head_list, data_list, {}, email2nickname, ws, 0)
    except Exception as e:
        dtable_io_logger.error('head_list = {}\n{}'.format(head_list, e))
        return
    wb.save(target_path)

def app_user_sync(dtable_uuid, app_name, app_id, table_name, table_id, username, config):
    dtable_io_logger.info('Start sync app %s users: to table %s.' % (app_name, table_name))
    db_session = init_db_session_class(config)()
    try:
        sync_app_users_to_table(dtable_uuid, app_id, table_name, table_id, username, db_session)
    except Exception as e:

        dtable_io_logger.exception('app user sync ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('app %s user sync success!' % app_name)
    finally:
        if db_session:
            db_session.close()


def email_sync(context, config):
    dtable_data_sync_logger.info('Start sync email to dtable %s, email table %s.' % (context.get('dtable_uuid'), context.get('detail',{}).get('email_table_id')))
    db_session = init_db_session_class(config)()
    context['db_session'] = db_session

    try:
        run_sync_emails(context)
    except Exception as e:
        dtable_data_sync_logger.exception('sync email ERROR: {}'.format(e))
    else:
        dtable_data_sync_logger.info('sync email success, sync_id: %s' % context.get('data_sync_id'))
    finally:
        if db_session:
            db_session.close()


def plugin_email_send_email(context, config=None):
    dtable_plugin_email_logger.info('Start send email by plugin %s, email table %s.' % (context.get('dtable_uuid'), context.get('table_info', {}).get('email_table_name')))

    dtable_uuid = context.get('dtable_uuid')
    username = context.get('username')
    repo_id = context.get('repo_id')
    workspace_id = context.get('workspace_id')

    table_info = context.get('table_info')
    email_info = context.get('email_info')
    auth_info = context.get('auth_info')

    thread_row_id = table_info.get('thread_row_id')
    email_row_id = table_info.get('email_row_id')
    email_table_name = table_info.get('email_table_name')
    thread_table_name = table_info.get('thread_table_name')

    # send email
    result = send_email_msg(auth_info, email_info, username, config)

    if result.get('err_msg'):
        dtable_plugin_email_logger.error('plugin email send failed, email account: %s, username: %s', auth_info.get('host_user'), username)
        return

    send_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    api_url = get_inner_dtable_server_url()
    dtable_server_api = DTableServerAPI(username, dtable_uuid, api_url, server_url=DTABLE_WEB_SERVICE_URL,
                                        repo_id=repo_id, workspace_id=workspace_id)

    replied_email_row = dtable_server_api.get_row(email_table_name, email_row_id)

    thread_id = replied_email_row.get('Thread ID')

    html_message = email_info.get('html_message')
    if html_message:
        html_message = '```' + html_message + '```'

    email = {
        'cc': email_info.get('copy_to'),
        'From': email_info.get('from'),
        'Message ID': email_info.get('message_id'),
        'Reply to Message ID': email_info.get('in_reply_to'),
        'To': email_info.get('send_to'),
        'Subject': email_info.get('subject'),
        'Content': email_info.get('text_message'),
        'HTML Content': html_message,
        'Date': send_time,
        'Thread ID': thread_id,
    }

    metadata = dtable_server_api.get_metadata()

    tables = metadata.get('tables', [])
    email_table_id = ''
    link_table_id = ''
    for table in tables:
        if table.get('name') == email_table_name:
            email_table_id = table.get('_id')
        if table.get('name') == thread_table_name:
            link_table_id = table.get('_id')
        if email_table_id and link_table_id:
            break
    if not email_table_id or not link_table_id:
        dtable_plugin_email_logger.error('email table: %s or link table: %s not found', email_table_name, thread_table_name)
        return

    email_link_id = dtable_server_api.get_column_link_id(email_table_name, 'Threads')

    email_row = dtable_server_api.append_row(email_table_name, email)
    email_row_id = email_row.get('_id')
    other_rows_ids = [thread_row_id]

    dtable_server_api.update_link(email_link_id, email_table_id, link_table_id, email_row_id, other_rows_ids)

    dtable_server_api.update_row(thread_table_name, thread_row_id, {'Last Updated': send_time})

def import_big_excel(username, dtable_uuid, table_name, file_path, task_id, tasks_status_map):
    """
    upload excel json file to dtable-db
    """

    dtable_io_logger.info('Start import big excel: {}.'.format(dtable_uuid))
    try:
        import_excel_to_db(username, dtable_uuid, table_name, file_path, task_id, tasks_status_map)
    except Exception as e:
        dtable_io_logger.error('import big excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('import big excel %s.xlsx success!' % table_name)


def update_big_excel(username, dtable_uuid, table_name, file_path, ref_columns, is_insert_new_data, task_id, tasks_status_map):
    """
    upload excel json file to dtable-db
    """

    dtable_io_logger.info('Start update big excel: {}.'.format(dtable_uuid))
    try:
        update_excel_to_db(username, dtable_uuid, table_name, file_path, ref_columns, is_insert_new_data, task_id, tasks_status_map)
    except Exception as e:
        dtable_io_logger.error('update big excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('update big excel %s.xlsx success!' % table_name)
