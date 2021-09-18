import base64
import json
import os
import shutil
import time

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

from dtable_events.dtable_io.utils import setup_logger, prepare_dtable_json, \
    prepare_asset_file_folder, post_dtable_json, post_asset_files, \
    download_files_to_path, create_forms_from_src_dtable, copy_src_forms_to_json, prepare_dtable_json_from_memory
from dtable_events.db import init_db_session_class
from dtable_events.dtable_io.excel import parse_excel_to_json, import_excel_by_dtable_server,\
    append_parsed_file_by_dtable_server, parse_append_excel_upload_excel_to_json,\
    update_parsed_file_by_dtable_server, parse_update_excel_upload_excel_to_json, parse_update_csv_upload_csv_to_json
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.statistics.db import save_email_sending_records

dtable_io_logger = setup_logger('dtable_events_io.log')
dtable_message_logger = setup_logger('dtable_events_message.log')

def clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path):
    # delete tmp files/dirs
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)
    if os.path.exists(tmp_zip_path):
        os.remove(tmp_zip_path)

def get_dtable_export_content(username, repo_id, dtable_uuid, asset_dir_id, config):
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

    dtable_io_logger.info('Clear tmp dirs and files before prepare.')
    clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path)
    os.makedirs(tmp_file_path, exist_ok=True)
    # import here to avoid circular dependency

    # 1. create 'content.json' from 'xxx.dtable'
    dtable_io_logger.info('Create content.json file.')
    try:
        prepare_dtable_json_from_memory(dtable_uuid, username)

    except Exception as e:
        dtable_io_logger.error('prepare dtable json failed. ERROR: {}'.format(e))

    # 2. get asset file folder, asset could be empty
    if asset_dir_id:
        dtable_io_logger.info('Create asset dir.')
        try:
            prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id)
        except Exception as e:
            dtable_io_logger.error('create asset folder failed. ERROR: {}'.format(e))

    # 3. copy forms
    try:
        copy_src_forms_to_json(dtable_uuid, tmp_file_path, db_session)
    except Exception as e:
        dtable_io_logger.error('copy forms failed. ERROR: {}'.format(e))


    """
    /tmp/dtable-io/<dtable_uuid>/dtable_asset/
                                    |- asset/
                                    |- content.json

    we zip /tmp/dtable-io/<dtable_uuid>/dtable_asset/ to /tmp/dtable-io/<dtable_id>/zip_file.zip and download it
    notice than make_archive will auto add .zip suffix to /tmp/dtable-io/<dtable_id>/zip_file
    """
    dtable_io_logger.info('Make zip file for download...')
    try:
        shutil.make_archive('/tmp/dtable-io/' + dtable_uuid +  '/zip_file', "zip", root_dir=tmp_file_path)
    except Exception as e:
        dtable_io_logger.error('make zip failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Create /tmp/dtable-io/{}/zip_file.zip success!'.format(dtable_uuid))
    # we remove '/tmp/dtable-io/<dtable_uuid>' in dtable web api


def post_dtable_import_files(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, config):
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
        post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name)
    except Exception as e:
        dtable_io_logger.error('post dtable json failed. ERROR: {}'.format(e))

    dtable_io_logger.info('Post asset files in tmp path to file server.')
    try:
        post_asset_files(repo_id, dtable_uuid, username)
    except Exception as e:
        dtable_io_logger.error('post asset files faield. ERROR: {}'.format(e))

    dtable_io_logger.info('create forms from src dtable.')
    try:
        create_forms_from_src_dtable(workspace_id, dtable_uuid, db_session)
    except Exception as e:
        dtable_io_logger.error('create forms failed. ERROR: {}'.format(e))

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

def parse_excel(username, repo_id, workspace_id, dtable_name, custom, config):
    """
    parse excel to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse excel: %s.xlsx.' % dtable_name)
    try:
        parse_excel_to_json(repo_id, dtable_name, custom)
    except Exception as e:
        dtable_io_logger.exception('parse excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('parse excel %s.xlsx success!' % dtable_name)

def import_excel(username, repo_id, workspace_id, dtable_uuid, dtable_name, config):
    """
    upload excel json file to dtable-server
    """
    dtable_io_logger.info('Start import excel: {}.'.format(dtable_uuid))
    try:
        import_excel_by_dtable_server(username, repo_id, dtable_uuid, dtable_name)
    except Exception as e:
        dtable_io_logger.error('import excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('import excel %s.xlsx success!' % dtable_name)

def append_excel_append_parsed_file(username, repo_id, dtable_uuid, file_name, table_name):
    """
    upload excel json file to dtable-server
    """
    dtable_io_logger.info('Start import excel: {}.'.format(dtable_uuid))
    try:
        append_parsed_file_by_dtable_server(username, repo_id, dtable_uuid, file_name, table_name)
    except Exception as e:
        dtable_io_logger.error('append excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('append excel %s.xlsx success!' % file_name)

def append_excel_upload_excel(username, repo_id, file_name, dtable_uuid, table_name):
    """
    parse excel to json file, then upload json file to file server
    """
    dtable_io_logger.info('Start parse append excel: %s.xlsx.' % file_name)
    try:
        parse_append_excel_upload_excel_to_json(repo_id, file_name, username, dtable_uuid, table_name)
    except Exception as e:
        dtable_io_logger.exception('parse append excel failed. ERROR: {}'.format(e))
    else:
        dtable_io_logger.info('parse append excel %s.xlsx success!' % file_name)

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

def send_wechat_msg(webhook_url, msg):
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

def send_email_msg(auth_info, send_info, username, config=None, db_session=None):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    # auth info
    email_host = auth_info.get('email_host')
    email_port = int(auth_info.get('email_port'))
    host_user = auth_info.get('host_user')
    password = auth_info.get('password')

    # send info
    msg = send_info.get('message', '')
    send_to = send_info.get('send_to', [])
    subject = send_info.get('subject', '')
    source = send_info.get('source', '')
    copy_to = send_info.get('copy_to', [])
    reply_to = send_info.get('reply_to', '')

    msg_obj = MIMEMultipart()
    content_body = MIMEText(msg)
    msg_obj['Subject'] = subject
    msg_obj['From'] = source or host_user
    msg_obj['To'] = ",".join(send_to)
    msg_obj['Cc'] = copy_to and ",".join(copy_to) or ""
    msg_obj['Reply-to'] = reply_to
    msg_obj.attach(content_body)

    result = {}
    try:
        smtp = smtplib.SMTP(email_host, int(email_port), timeout=5)
    except Exception as e:
        dtable_message_logger.error(
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
    except Exception as e :
        dtable_message_logger.error(
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


def convert_page_to_pdf(dtable_uuid, page_id, row_id, access_token, session_id):
    if not row_id:
        url = task_manager.conf['dtable_web_service_url'].strip('/') + '/dtable/%s/page-design/%s/' % (dtable_uuid, page_id)
    if row_id:
        url = task_manager.conf['dtable_web_service_url'].strip('/') + '/dtable/%s/page-design/%s/row/%s/' % (dtable_uuid, page_id, row_id)
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

    driver.get(task_manager.conf['dtable_web_service_url'])
    cookies = [{
        'name': task_manager.conf['session_cookie_name'],
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

    try:
        # make sure react is rendered, timeout 60s
        WebDriverWait(driver, 60).until(lambda driver: driver.find_element_by_class_name('page-design-record') is not None, message='wait react timeout')
        # make sure images from asset are rendered, timeout 120s
        WebDriverWait(driver, 120, poll_frequency=1).until(lambda driver: check_images_and_networks(driver), message='wait images and networks timeout')
        time.sleep(2) # wait for fonts rendering
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
