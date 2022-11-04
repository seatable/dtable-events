import time
import re
import ssl
import socket
import logging
from datetime import timedelta
from datetime import datetime
from uuid import uuid4
from imapclient.exceptions import LoginError

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_DB_URL
from dtable_events.automations.models import get_third_party_account
from dtable_events.data_sync.imap_mail import ImapMail
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.dtable_server_api import DTableServerAPI


logger = logging.getLogger(__name__)


REQUIRED_EMAIL_COLUMNS = ['From', 'Message ID', 'To', 'Subject', 'cc', 'Date', 'Reply to Message ID', 'Thread ID']
REQUIRED_THREAD_COLUMNS = ['Subject', 'Last Updated', 'Thread ID']

def login_imap(host, user, password, port=None, timeout=None):
    imap = ImapMail(host, user, password, port=port, ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLSv1_2), timeout=timeout)
    imap.client()
    logger.debug('imap: %s client successfully!', host)
    imap.login()
    logger.debug('imap_host: %s email_user: %s, login imap client successfully!', host, user)
    return imap


def check_imap_account(imap_server, email_user, email_password, port=None, return_imap=False, timeout=None):
    """
    check imap server user and password

    return: error_msg -> str or None
    """
    try:
        imap = login_imap(imap_server, email_user, email_password, port=port, timeout=timeout)
    except LoginError:
        if not return_imap:
            return 'user or password invalid, email: %s user login error' % (email_user,)
        else:
            return None, 'user or password invalid, email: %s user login error' % (email_user,)
    except Exception as e:
        logger.exception(e)
        logger.error('imap_server: %s, email_user: %s, login error: %s' % (imap_server, email_user, e))
        if not return_imap:
            return 'email: %s login error: %s' % (email_user, e)
        else:
            return None, 'email: %s login error: %s' % (email_user, e)

    if not return_imap:
        imap.close()
        return None
    else:
        return imap, None


def fixed_sql_query(seatable, sql):
    try:
        return seatable.query(sql)
    except TypeError:
        return []


def query_table_rows(dtable_db_api, table_name, fields='*', conditions='', all=True, limit=None):
    where_conditions = f"where {conditions}" if conditions else ''
    if all:
        result = fixed_sql_query(dtable_db_api, f"select count(*) from `{table_name}` {where_conditions}")[0]
        limit = result['COUNT(*)']
        if limit == 0:
            return []
    else:
        limit = 100 if not limit else limit
    return fixed_sql_query(dtable_db_api, f"select {fields} from `{table_name}` {where_conditions} limit {limit}")


def str_2_datetime(s: str):
    if '+' in s:
        s = s[:s.find('+')]
    formats = ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d']
    for f in formats:
        try:
            return datetime.strptime(s, f)
        except:
            pass
    raise Exception(f"date {s} can't be transfered to datetime")


def update_email_thread_ids(dtable_db_api, email_table_name, send_date, email_list):
    """
    return: email list, [email1, email2...], email is with thread id
    """
    # get email rows in last 30 days and generate message-thread dict {`Message ID`: `Thread ID`}
    last_month_day = (str_2_datetime(send_date) - timedelta(days=30)).strftime('%Y-%m-%d')
    email_rows = query_table_rows(dtable_db_api, email_table_name,
                                  fields='`Message ID`, `Thread ID`',
                                  conditions=f"Date>='{last_month_day}'")
    message2thread = {email['Message ID']: email['Thread ID'] for email in email_rows}

    email_list = [email for email in email_list if not message2thread.get(email['Message ID'])]

    # no_thread_reply_message_ids is the list of new emails' reply-ids who are not in last 30 days
    no_thread_reply_message_ids = []
    for email in email_list:
        if email['Reply to Message ID'] and not message2thread.get(email['Reply to Message ID']):
            no_thread_reply_message_ids.append(email['Reply to Message ID'])
    if no_thread_reply_message_ids:
        step = 100
        for i in range(0, len(no_thread_reply_message_ids), step):
            message_ids_str = ', '.join([f"'{message_id}'" for message_id in no_thread_reply_message_ids[i: i+step]])
            conditions = f"`Message ID`in ({message_ids_str})"
            earlier_email_rows = query_table_rows(dtable_db_api, email_table_name,
                                                  fields='`Message ID`, `Thread ID`',
                                                  conditions=conditions,
                                                  all=False,
                                                  limit=step)
            for email in earlier_email_rows:
                message2thread[email['Message ID']] = email['Thread ID']

    new_thread_rows = []
    to_be_updated_thread_dict = {}
    # update email thread id
    for email in email_list:
        reply_to_id = email['Reply to Message ID']
        message_id = email['Message ID']
        if reply_to_id in message2thread:  # checkout thread id from old message2thread
            thread_id = message2thread[reply_to_id]
            message2thread[message_id] = thread_id
            if thread_id in to_be_updated_thread_dict:
                # update Last Updated
                if str_2_datetime(email['Date']) > str_2_datetime(to_be_updated_thread_dict[thread_id]['Last Updated']):
                    to_be_updated_thread_dict[thread_id]['Last Updated'] = email['Date']
                # append email message id
                to_be_updated_thread_dict[thread_id]['message_ids'].append(message_id)
            else:
                to_be_updated_thread_dict[thread_id] = {
                    'Last Updated': email['Date'],
                    'message_ids': [message_id]
                }
            if not email.get('is_sender'):
                to_be_updated_thread_dict[thread_id]['Unread'] = True
        else:  # generate new thread id
            thread_id = uuid4().hex
            message2thread[message_id] = thread_id
            if email.get('is_sender'):
                new_thread_rows.append({
                    'Subject': email['Subject'],
                    'Last Updated': email['Date'],
                    'Thread ID': thread_id
                })
                to_be_updated_thread_dict[thread_id] = {
                    'Last Updated': email['Date'],
                    'message_ids': [message_id]
                }
            else:
                new_thread_rows.append({
                    'Subject': email['Subject'],
                    'Last Updated': email['Date'],
                    'Thread ID': thread_id,
                    'Unread': True
                })
                to_be_updated_thread_dict[thread_id] = {
                    'Last Updated': email['Date'],
                    'message_ids': [message_id],
                    'Unread': True
                }
        email['Thread ID'] = message2thread[message_id]

    return email_list, new_thread_rows, to_be_updated_thread_dict


def fill_email_list_with_row_id(dtable_db_api, email_table_name, email_list):
    step = 100
    message_id_row_id_dict = {}  # {message_id: row._id}
    for i in range(0, len(email_list), step):
        message_ids_str = ', '.join([f"'{email['Message ID']}'" for email in email_list[i: i+step]])
        conditions = f'`Message ID` in ({message_ids_str})'
        email_rows = query_table_rows(dtable_db_api, email_table_name,
                                      fields='`_id`, `Message ID`',
                                      conditions=conditions,
                                      all=False,
                                      limit=step)
        message_id_row_id_dict.update({row['Message ID']: {
            '_id': row['_id'],
        } for row in email_rows})
    for email in email_list:
        email['_id'] = message_id_row_id_dict[email['Message ID']]['_id']
    return email_list


def get_thread_email_ids(thread_row_emails):
    if thread_row_emails is None:
        return []
    return [email['row_id'] for email in thread_row_emails]


def update_threads(seatable, dtable_db_api, email_table_name, link_table_name, email_list, to_be_updated_thread_dict):
    """
    update thread table
    email_list: list of email
    to_be_updated_thread_dict: {thread_id: {'Last Updated': 'YYYY-MM-DD', 'message_ids': [message_id1, message_id2...]}}
    """
    to_be_updated_thread_ids = list(to_be_updated_thread_dict.keys())
    thread_id_row_id_dict = {}
    step = 100
    for i in range(0, len(to_be_updated_thread_ids), step):
        thread_ids_str = ', '.join([f"'{thread_id}'" for thread_id in to_be_updated_thread_ids[i: i+step]])
        conditions = f"`Thread ID` in ({thread_ids_str})"
        thread_rows = query_table_rows(dtable_db_api, link_table_name,
                                       fields='`Thread ID`, `_id`, `Emails`',
                                       conditions=conditions,
                                       all=False,
                                       limit=step)
        thread_id_row_id_dict.update({row['Thread ID']: [row['_id'], get_thread_email_ids(row.get('Emails'))] for row in thread_rows})

    # batch update Last Updated
    to_be_updated_last_updated_rows = [{
        'row_id': thread_id_row_id_dict[key][0],
        'row': {'Last Updated': value['Last Updated'], 'Unread': True}
    } if value.get('Unread') else {
        'row_id': thread_id_row_id_dict[key][0],
        'row': {'Last Updated': value['Last Updated']}
    } for key, value in to_be_updated_thread_dict.items()]
    seatable.batch_update_rows(link_table_name, to_be_updated_last_updated_rows)

    # fill email in email_list with row id
    email_list = fill_email_list_with_row_id(dtable_db_api, email_table_name, email_list)
    email_dict = {email['Message ID']: email for email in email_list}
    # add link
    link_id = seatable.get_column_link_id(link_table_name, 'Emails', view_name=None)

    other_rows_ids_map = {}
    row_id_list = []

    for thread_id, value in to_be_updated_thread_dict.items():
        row_id = thread_id_row_id_dict[thread_id][0]
        row_id_list.append(row_id)
        other_rows_ids_map[row_id] = thread_id_row_id_dict[thread_id][1]
        for message_id in value['message_ids']:
            other_rows_ids_map[row_id].append(email_dict[message_id]['_id'])

    tables = seatable.get_metadata()
    table_info = {table['name']: table['_id'] for table in tables['tables']}
    link_table_id = table_info[link_table_name]
    email_table_id = table_info[email_table_name]

    seatable.batch_update_links(link_id, link_table_id, email_table_id, row_id_list, other_rows_ids_map)


def update_emails(seatable, dtable_db_api, email_table_name, email_list):
    """
    update email table
    email_list: list of email
    """
    to_be_updated_attachments_dict = {email['Message ID']: email['Attachment'] for email in email_list if
                                      email['Attachment']}
    to_be_updated_message_ids = list(to_be_updated_attachments_dict.keys())

    message_id_row_id_dict = {}
    step = 100
    for i in range(0, len(to_be_updated_message_ids), step):
        message_ids_str = ', '.join([f"'{message_id}'" for message_id in to_be_updated_message_ids[i: i + step]])
        conditions = f"`Message ID` in ({message_ids_str})"
        email_rows = query_table_rows(dtable_db_api, email_table_name,
                                      fields='`Message ID`, `_id`',
                                      conditions=conditions,
                                      all=False,
                                      limit=step)
        message_id_row_id_dict.update({row['Message ID']: row['_id'] for row in email_rows})

    message_id_attachment_dict = {}
    for email_message_id in to_be_updated_attachments_dict:
        attachments = to_be_updated_attachments_dict[email_message_id]
        attachment_list = []
        for attachment_info_dict in attachments:
            attachment_list.append(attachment_info_dict)
        message_id_attachment_dict[email_message_id] = attachment_list

    to_be_updated_attachment_rows = [{
        'row_id': message_id_row_id_dict[key],
        'row': {'Attachment': value}
    } for key, value in message_id_attachment_dict.items()]

    # update attachment rows
    seatable.batch_update_rows(email_table_name, to_be_updated_attachment_rows)


def upload_attachments(seatable, email_list):
    for email in email_list:
        file_list = email.pop('Attachment', [])
        filename2content_id = email.pop('filename2content_id', {})
        html_content = email.pop('HTML Content', '')
        filename2url = {}
        file_info_list = []
        for file in file_list:
            file_name = file.get('file_name')
            file_data = file.get('file_data')
            try:
                file_info = seatable.upload_bytes_file(file_name, file_data)
                file_info_list.append(file_info)
                filename2url[file_name] = file_info['url']
            except Exception as e:
                logger.error('upload email: %s attachment: %s error: %s', email.get('Message ID'), file_name, e)
        email['Attachment'] = file_info_list

        # deal html content image
        # replace cid with real image url
        for file_name in filename2content_id:
            repl = filename2url.get(file_name)
            # repl maybe None if upload attachment fail
            if not repl:
                continue
            rep = re.compile(r'cid:%s' % re.escape(filename2content_id[file_name]))
            html_content = rep.sub(repl, html_content, 0)
        email['HTML Content'] = html_content
    return email_list


def sync_email_to_table(seatable, dtable_db_api, email_table_name, link_table_name, send_date, email_list):
    # update thread id of emails
    email_list, new_thread_rows, to_be_updated_thread_dict = update_email_thread_ids(dtable_db_api, email_table_name,
                                                                                     send_date, email_list)
    logger.info(f'table: {email_table_name}, need to be inserted {len(email_list)} emails')
    logger.info(f'table: {link_table_name}, need to be inserted {len(new_thread_rows)} thread rows')

    # upload attachments
    email_list = upload_attachments(seatable, email_list)
    # insert new emails
    seatable.batch_append_rows(email_table_name, email_list)

    # wait several seconds for dtable-db
    time.sleep(2)
    # update attachment
    update_emails(seatable, dtable_db_api, email_table_name, email_list)
    # insert new thread rows
    if new_thread_rows:
        seatable.batch_append_rows(link_table_name, new_thread_rows)

    # wait several seconds for dtable-db
    time.sleep(3)

    # update threads Last Updated and Emails
    update_threads(seatable, dtable_db_api, email_table_name, link_table_name, email_list, to_be_updated_thread_dict)


def set_data_sync_invalid(data_sync_id, db_session):
    sql = "UPDATE dtable_data_syncs SET is_valid=0 WHERE id =:data_sync_id"

    db_session.execute(sql, {'data_sync_id': data_sync_id})
    db_session.commit()


def update_sync_time(data_sync_id, db_session):
    sql = "UPDATE dtable_data_syncs SET last_sync_time=:last_sync_time WHERE id =:data_sync_id"

    db_session.execute(sql, {'data_sync_id': data_sync_id, 'last_sync_time': datetime.now()})
    db_session.commit()


def run_sync_emails(context):
    data_sync_id = context['data_sync_id']
    dtable_uuid = context['dtable_uuid']
    detail = context['detail']
    repo_id = context['repo_id']
    workspace_id = context['workspace_id']
    db_session = context['db_session']

    send_date = context.get('send_date')
    username = context.get('username', 'Data Sync')

    api_url = get_inner_dtable_server_url()

    account_id = detail.get('third_account_id')
    email_table_id = detail.get('email_table_id')
    link_table_id = detail.get('link_table_id')

    if not all([account_id, email_table_id, link_table_id]):
        set_data_sync_invalid(data_sync_id, db_session)
        logger.warning('account settings invalid.')
        return

    if not send_date:
        send_date = str(datetime.today().date())
        if str(datetime.today().hour) == '0':
            send_date = str((datetime.today() - timedelta(days=1)).date())
    else:
        try:
            if datetime.strptime(send_date, '%Y-%m-%d').date() > datetime.today().date():
                return
        except:
            logger.error('send_date invalid.')
            return

    account = get_third_party_account(db_session, account_id)
    account_type = account.get('account_type')
    account_detail = account.get('detail')
    if not account or account_type != 'email' or not account_detail:
        set_data_sync_invalid(data_sync_id, db_session)
        logger.warning('third party account not found.')
        return

    imap_host = account_detail.get('imap_host')
    imap_port = account_detail.get('imap_port')
    email_user = account_detail.get('host_user')
    email_password = account_detail.get('password')
    if not all([imap_host, imap_port, email_user, email_password]):
        set_data_sync_invalid(data_sync_id, db_session)
        logger.warning('third party account invalid.')
        return

    # check imap account
    try:
        imap = login_imap(imap_host, email_user, email_password, port=imap_port)
    except LoginError:
        logger.warning('user or password invalid, email: %s user login error', email_user)
        set_data_sync_invalid(data_sync_id, db_session)
        return
    except Exception as e:
        logger.exception(e)
        logger.error('imap_server: %s, email_user: %s, login error: %s', imap_host, email_user, e)
        return

    dtable_server_api = DTableServerAPI(username, dtable_uuid, api_url,
                                        server_url=DTABLE_WEB_SERVICE_URL,
                                        repo_id=repo_id,
                                        workspace_id=workspace_id
                                        )

    dtable_db_api = DTableDBAPI(username, dtable_uuid, INNER_DTABLE_DB_URL)

    metadata = dtable_server_api.get_metadata()

    email_table_name = ''
    link_table_name = ''
    email_columns = []
    link_columns = []

    tables = metadata.get('tables', [])
    for table in tables:
        if not email_table_name and table.get('_id') == email_table_id:
            email_table_name = table.get('name')
            email_columns = table.get('columns')
        if not link_table_name and table.get('_id') == link_table_id:
            link_table_name = table.get('name')
            link_columns = table.get('columns')
        if email_table_name and link_table_name:
            break

    if not email_table_name or not link_table_name:
        set_data_sync_invalid(data_sync_id, db_session)
        logger.warning('email table or link table invalid.')
        return

    # check required columns
    email_columns_dict = {column.get('name'): True for column in email_columns}
    link_columns_dict = {column.get('name'): True for column in link_columns}

    for col_name in REQUIRED_EMAIL_COLUMNS:
        if not email_columns_dict.get(col_name):
            set_data_sync_invalid(data_sync_id, db_session)
            logger.warning('email table no such column: %s', col_name)
            return

    for col_name in REQUIRED_THREAD_COLUMNS:
        if not link_columns_dict.get(col_name):
            set_data_sync_invalid(data_sync_id, db_session)
            logger.warning('thread table no such column: %s', col_name)
            return

    try:
        email_list = sorted(imap.search_emails_by_send_date(send_date, 'SINCE'), key=lambda x: str_2_datetime(x['Date']))
    except socket.timeout as e:
        logger.exception(e)
        logger.error('email: %s get emails timeout: %s', email_user, e)
        return

    logger.info(f'email: {email_user} fetch {len(email_list)} emails')

    try:
        sync_email_to_table(dtable_server_api, dtable_db_api, email_table_name, link_table_name, send_date, email_list)
    except Exception as e:
        logger.exception(e)
        logger.error('email: %s sync and update link error: %s', email_user, e)
        return
    update_sync_time(data_sync_id, db_session)
