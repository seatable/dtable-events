import shutil
import os
import requests
from dtable_events.dtable_io.utils import setup_logger, prepare_dtable_json, \
    prepare_asset_file_folder, post_dtable_json, post_asset_files, \
    download_files_to_path, create_forms_from_src_dtable, copy_src_forms_to_json
from dtable_events.db import init_db_session_class

dtable_io_logger = setup_logger()


def clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path):
    # delete tmp files/dirs
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)
    if os.path.exists(tmp_zip_path):
        os.remove(tmp_zip_path)

def get_dtable_export_content(username, repo_id, table_name, dtable_uuid, dtable_file_dir_id, asset_dir_id, config):
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
        prepare_dtable_json(repo_id, dtable_uuid, table_name, dtable_file_dir_id)
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

    # delete local tmp files
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)

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