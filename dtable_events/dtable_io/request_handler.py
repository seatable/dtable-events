import json
import jwt
import logging

from flask import Flask, request, make_response

from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager

app = Flask(__name__)
logger = logging.getLogger(__name__)


def check_auth_token(req):
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() != 'token' or len(auth) != 2:
        return False, 'Token invalid.'

    token = auth[1]
    if not token:
        return False, 'Token invalid.'

    private_key = task_manager.conf['dtable_private_key']
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        return False, e

    return True, None


@app.route('/add-export-task', methods=['GET'])
def add_export_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    table_name = request.args.get('table_name')
    dtable_uuid = request.args.get('dtable_uuid')

    try:
        task_id = task_manager.add_export_task(
            username, repo_id, dtable_uuid, table_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-task', methods=['GET'])
def add_import_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_file_name = request.args.get('dtable_file_name')

    try:
        task_id = task_manager.add_import_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_file_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-parse-excel-task', methods=['GET'])
def add_parse_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_name = request.args.get('dtable_name')
    custom = request.args.get('custom')
    custom = bool(int(custom))

    try:
        task_id = task_manager.add_parse_excel_task(
            username, repo_id, workspace_id, dtable_name, custom)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-task', methods=['GET'])
def add_import_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_name = request.args.get('dtable_name')

    try:
        task_id = task_manager.add_import_excel_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-add-table-task', methods=['GET'])
def add_import_excel_add_table_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_name = request.args.get('dtable_name')

    try:
        task_id = task_manager.add_import_excel_add_table_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-append-excel-append-parsed-file-task', methods=['GET'])
def add_append_excel_append_parsed_file_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    file_name = request.args.get('file_name')
    table_name = request.args.get('table_name')

    try:
        task_id = task_manager.add_append_excel_append_parsed_file_task(
            username, repo_id, dtable_uuid, file_name, table_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-append-excel-upload-excel-task', methods=['GET'])
def add_append_excel_upload_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    table_name = request.args.get('table_name')

    try:
        task_id = task_manager.add_append_excel_upload_excel_task(
            username, repo_id, file_name, dtable_uuid, table_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/query-status', methods=['GET'])
def query_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished = task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)  # task_id not found
        return make_response((e, 500))

    return make_response(({'is_finished': is_finished}, 200))


@app.route('/cancel-task', methods=['GET'])
def cancel_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        task_manager.cancel_task(task_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'success': True}, 200))


@app.route('/query-message-send-status', methods=['GET'])
def query_message_send_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not message_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished, result = message_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)  # task_id not found
        return make_response((e, 500))

    resp = dict(is_finished=is_finished)
    resp['result'] = result if result else {}
    return make_response((resp, 200))


@app.route('/cancel-message-send-task', methods=['GET'])
def cancel_message_send_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not message_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        message_task_manager.cancel_task(task_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'success': True}, 200))


@app.route('/convert-page-to-pdf', methods=['GET'])
def convert_page_to_pdf():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')
    row_id = request.args.get('row_id')
    access_token = request.args.get('access_token')
    session_id = request.args.get('session_id')

    try:
        task_id = task_manager.convert_page_to_pdf(
            dtable_uuid, page_id, row_id, access_token, session_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/insert-page-to-row', methods=['GET'])
def insert_page_to_row():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')
    row_id = request.args.get('row_id')
    access_token = request.args.get('access_token')
    session_id = request.args.get('session_id')
    target_table_name = request.args.get('target_table_name')

    target_row = request.args.get('target_row')

    target_column_name = request.args.get('target_column_name')
    file_name = request.args.get('file_name')
    server_url = request.args.get('server_url')
    if not isinstance(target_row, dict):
        target_row = json.loads(target_row)


    try:
        task_id = task_manager.insert_page_to_row(
            dtable_uuid, page_id, row_id, access_token, session_id, target_table_name, target_row, target_column_name, file_name, server_url)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/dtable-asset-files', methods=['POST'])
def dtable_asset_files():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    repo_id = data.get('repo_id')
    dtable_uuid = data.get('dtable_uuid')
    files = data.getlist('files')
    files_map = data.get('files_map')

    if not isinstance(files, list):
        files = [files]
    if not isinstance(files_map, dict):
        files_map = json.loads(files_map)

    try:
        task_id = task_manager.add_export_dtable_asset_files_task(
            username, repo_id, dtable_uuid, files, files_map)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/transfer-dtable-asset-files', methods=['POST'])
def transfer_dtable_asset_files():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    repo_id = data.get('repo_id')
    dtable_uuid = data.get('dtable_uuid')
    files = data.getlist('files')
    files_map = data.get('files_map')
    repo_api_token = data.get('repo_api_token')
    seafile_server_url = data.get('seafile_server_url')
    parent_dir = data.get('parent_dir')
    relative_path = data.get('relative_path')
    replace = data.get('replace')

    if not isinstance(files, list):
        files = [files]
    if not isinstance(files_map, dict):
        files_map = json.loads(files_map)

    try:
        task_id = task_manager.add_transfer_dtable_asset_files_task(
            username, repo_id, dtable_uuid, files, files_map, parent_dir,
            relative_path, replace, repo_api_token, seafile_server_url)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-wechat-sending-task', methods=['POST'])
def add_wechat_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    webhook_url = data.get('webhook_url')
    msg = data.get('msg')

    try:
        task_id = message_task_manager.add_wechat_sending_task(webhook_url, msg)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-email-sending-task', methods=['POST'])
def add_email_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    send_to = data.getlist('send_to')
    copy_to = data.getlist('copy_to')

    if not isinstance(send_to, list):
        send_to = [send_to]
    if copy_to and not isinstance(copy_to, list):
        copy_to = [copy_to]

    auth_info = {
        'email_host': data.get('email_host'),
        'email_port': data.get('email_port'),
        'host_user': data.get('host_user'),
        'password': data.get('password')
    }

    send_info = {
        'message': data.get('message'),
        'send_to': send_to,
        'subject': data.get('subject'),
        'source': data.get('source'),
        'copy_to': copy_to,
        'reply_to': data.get('reply_to'),
    }

    try:
        task_id = message_task_manager.add_email_sending_task(
            auth_info, send_info, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-run-auto-rule-task', methods=['POST'])
def add_run_auto_rule_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    org_id = data.get('org_id')
    run_condition = data.get('run_condition')
    trigger = data.get('trigger')
    dtable_uuid = data.get('dtable_uuid')
    actions = data.get('actions')

    try:
        task_id = task_manager.add_run_auto_rule_task(
            username, org_id, dtable_uuid, run_condition, trigger, actions)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))
