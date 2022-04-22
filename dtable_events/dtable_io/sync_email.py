import time
import socket
from dtable_events.dtable_io import dtable_io_logger
from dtable_events.dtable_io.utils import get_emails, str_2_datetime, update_email_thread_ids, upload_attachments, \
    update_emails, update_threads, get_seatable_api


def sync_email(context):
    imap_host = context['imap_host']
    imap_port = context['imap_port']
    email_user = context['email_user']
    email_password = context['email_password']

    send_date = context['send_date']
    email_table_name = context['email_table_name']
    link_table_name = context['link_table_name']
    mode = context['mode']
    username = context['username']
    dtable_uuid = context['dtable_uuid']

    try:
        seatable = get_seatable_api(username, dtable_uuid)
    except Exception as e:
        dtable_io_logger.error('get seatable error:%s', e)
        return

    try:
        # get emails on send_date
        email_list = sorted(get_emails(send_date, imap_host, email_user, email_password, port=imap_port, mode=mode),
                            key=lambda x: str_2_datetime(x['Date']))
        if not email_list:
            dtable_io_logger.info('email: %s send_date: %s mode: %s get 0 email(s)', email_user, send_date, mode)
            return
    except socket.timeout as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('email: %s get emails timeout: %s', email_user, e)
        return

    dtable_io_logger.info(f'email: {email_user} fetch {len(email_list)} emails')

    try:
        # update thread id of emails
        email_list, new_thread_rows, to_be_updated_thread_dict = update_email_thread_ids(seatable, email_table_name,
                                                                                         send_date, email_list)
        dtable_io_logger.info(f'email: {email_user}, need to be inserted {len(email_list)} emails')
        dtable_io_logger.info(f'email: {email_user}, need to be inserted {len(new_thread_rows)} thread rows')

        if not email_list:
            return

        # upload attachments
        email_list = upload_attachments(seatable, email_list)
        # insert new emails
        seatable.batch_append_rows(email_table_name, email_list)

        # wait several seconds for dtable-db
        time.sleep(2)
        # update attachment
        update_emails(seatable, email_table_name, email_list)
        # insert new thread rows
        if new_thread_rows:
            seatable.batch_append_rows(link_table_name, new_thread_rows)

        # wait several seconds for dtable-db
        time.sleep(3)

        # update threads Last Updated and Emails
        update_threads(seatable, email_table_name, link_table_name, email_list, to_be_updated_thread_dict)
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('email: %s sync and update link error: %s', email_user, e)
