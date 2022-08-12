import json
import logging
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, uuid_str_to_36_chars, get_inner_dtable_server_url
from dtable_events.data_sync.data_sync_utils import set_data_sync_invalid, sync_email, check_imap_account
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.app.config import INNER_DTABLE_DB_URL, DTABLE_WEB_SERVICE_URL
from dtable_events.automations.models import get_third_party_account

logger = logging.getLogger(__name__)


class DataSyncer(object):

    def __init__(self, config):
        self._enabled = True
        self._prepara_config(config)
        self._db_session_class = init_db_session_class(config)

    def _prepara_config(self, config):
        section_name = 'EMAIL-SYNCER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        self._enabled = parse_bool(enabled)

    def start(self):
        if not self.is_enabled():
            logging.warning('Email syncer not enabled')
            return
        DataSyncerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def list_pending_data_syncs(db_session):
    sql = '''
            SELECT das.id, das.dtable_uuid, das.sync_type, das.detail, w.repo_id, w.id FROM dtable_data_syncs das
            INNER JOIN dtables d ON das.dtable_uuid=d.uuid AND d.deleted=0
            INNER JOIN workspaces w ON w.id=d.workspace_id
             WHERE das.is_valid=1
        '''

    dataset_list = db_session.execute(sql)
    return dataset_list


def check_data_syncs(db_session):
    data_sync_list = list_pending_data_syncs(db_session)

    api_url = get_inner_dtable_server_url()

    for data_sync in data_sync_list:
        data_sync_id = data_sync[0]
        dtable_uuid = uuid_str_to_36_chars(data_sync[1])
        sync_type = data_sync[2]
        detail = json.loads(data_sync[3])
        repo_id = data_sync[4]
        workspace_id = data_sync[5]

        if sync_type == 'email':
            account_id = detail.get('third_account_id')
            email_table_id = detail.get('email_table_id')
            link_table_id = detail.get('link_table_id')

            if not all([account_id, email_table_id, link_table_id]):
                set_data_sync_invalid(data_sync_id, db_session)
                logging.error('account settings invalid.')
                return

            account = get_third_party_account(db_session, account_id)
            account_type = account.get('account_type')
            account_detail = account.get('detail')
            if not account or account_type != 'email' or not account_detail:
                set_data_sync_invalid(data_sync_id, db_session)
                logging.error('third party account not found.')
                return

            imap_host = account_detail.get('imap_host')
            imap_port = account_detail.get('imap_port')
            email_user = account_detail.get('host_user')
            email_password = account_detail.get('password')
            if not all([imap_host, imap_port, email_user, email_password]):
                set_data_sync_invalid(data_sync_id, db_session)
                logging.error('third party account invalid.')
                return

            imap, error_msg = check_imap_account(imap_host, email_user, email_password, port=imap_port, return_imap=True)

            if error_msg:
                set_data_sync_invalid(data_sync_id, db_session)
                logging.error(error_msg)
                return

            dtable_server_api = DTableServerAPI('Data Sync', dtable_uuid, api_url,
                                                server_url=DTABLE_WEB_SERVICE_URL,
                                                repo_id=repo_id,
                                                workspace_id=workspace_id
                                                )

            dtable_db_api = DTableDBAPI('Data Sync', dtable_uuid, INNER_DTABLE_DB_URL)

            metadata = dtable_server_api.get_metadata()

            email_table_name = ''
            link_table_name = ''

            tables = metadata.get('tables', [])
            for table in tables:
                if not email_table_name and table.get('_id') == email_table_id:
                    email_table_name = table.get('name')
                if not link_table_name and table.get('_id') == link_table_id:
                    link_table_name = table.get('name')
                if email_table_name and link_table_name:
                    break

            if not email_table_name or not link_table_name:
                set_data_sync_invalid(data_sync_id, db_session)
                logging.error('email table or link table invalid.')
                return

            send_date = str(datetime.today().date())

            if str(datetime.today().hour) == '0':
                send_date = str((datetime.today() - timedelta(days=1)).date())

            sync_info = {
                'imap_host': imap_host,
                'imap_port': imap_port,
                'email_user': email_user,
                'email_password': email_password,
                'send_date': send_date,
                'email_table_name': email_table_name,
                'link_table_name': link_table_name,
                'dtable_server_api': dtable_server_api,
                'dtable_db_api': dtable_db_api,
                'imap': imap,
            }

            sync_email(sync_info)


class DataSyncerTimer(Thread):
    def __init__(self, db_session_class):
        super(DataSyncerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at the 30th minute of every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='30')
        def timed_job():
            logging.info('Starts to scan data syncs...')
            db_session = self.db_session_class()
            try:
                check_data_syncs(db_session)
            except Exception as e:
                logger.exception('check periodical data syncs error: %s', e)
            finally:
                db_session.close()

        sched.start()
