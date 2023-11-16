import logging
from datetime import datetime
from uuid import uuid4

from sqlalchemy.orm import sessionmaker

from dtable_events.app.config import SYSTEM_BASES_OWNER, INNER_DTABLE_DB_URL, NEW_DTABLE_IN_STORAGE_SERVER
from dtable_events.system_bases.constants import VERSION_BASE_NAME, VERSION_TABLE_NAME, CDS_STATISTICS_BASE_NAME, CDS_STATISTICS_TABLE_NAME
from dtable_events.utils import uuid_str_to_36_chars, get_inner_dtable_server_url, gen_random_option
from dtable_events.utils.storage_backend import storage_backend
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.dtable_server_api import DTableServerAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()


class BasicBaseManager:

    base_name = ''
    table_name = ''
    columns = []

    def __init__(self, session_class: sessionmaker, workspace_id, repo_id):
        self.dtable_uuid = ''
        self.session_class = session_class
        self.workspace_id = workspace_id
        self.repo_id = repo_id
        self.is_ready = False

    def check_db_base(self):
        """
        :return: dtable_uuid
        """
        with self.session_class() as session:
            sql = '''
            SELECT uuid FROM dtables d
            JOIN workspaces w ON d.workspace_id=w.id
            WHERE w.owner=:owner AND d.name=:name
            '''
            result = session.execute(sql, {'owner': SYSTEM_BASES_OWNER, 'name': self.base_name}).fetch_one()
            if not result:
                return None
            self.dtable_uuid = uuid_str_to_36_chars(result.uuid)
            return self.dtable_uuid

    def get_table(self):
        dtable_server_api = self.get_dtable_server_api()
        metadata = dtable_server_api.get_metadata()
        table = next(filter(lambda x: x['name'] == self.table_name, metadata['tables']), None)
        return table

    def check_table(self) -> bool:
        """
        :return: is_table_valid
        """
        table = self.get_table()
        if not table:
            return False
        for column in self.columns:
            table_column = next(filter(lambda x: x['name'] == column['name']), None)
            if not table_column:
                return False
            if column['type'] != table_column['type']:
                return False
        return True

    def check(self):
        dtable_uuid = self.check_db_base()
        if not dtable_uuid:
            return False
        table = self.check_table()
        if not table:
            return False
        self.is_ready = True
        return True

    def create_db_base(self):
        self.dtable_uuid = str(uuid4())
        now = datetime.now()
        with self.session_class() as session:
            sql = '''
            INSERT INTO dtables(uuid, name, creator, modifier, created_at, updated_at, workspace_id, in_storage) VALUES
            (SELECT :dtable_uuid, :name, :owner, :owner, :created_at, :updated_at, :workspace_id, :in_storage)
            '''
            session.execute(sql, {
                'dtable_uuid': self.dtable_uuid,
                'name': self.base_name,
                'owner': SYSTEM_BASES_OWNER,
                'created_at': now,
                'updated_at': now,
                'workspace_id': self.workspace_id,
                'in_storage': 1 if NEW_DTABLE_IN_STORAGE_SERVER else 0
            })
            session.commit()

    def create_table(self):
        storage_backend.create_empty_dtable(self.dtable_uuid, SYSTEM_BASES_OWNER, NEW_DTABLE_IN_STORAGE_SERVER, self.repo_id, f'{self.base_name}.dtable')
        dtable_server_api = DTableServerAPI('dtable-events', self.dtable_uuid, dtable_server_url)
        dtable_server_api.add_table(self.base_name, columns=self.columns)
        try:
            dtable_server_api.delete_table_by_id('0000')
        except:
            pass
        dtable_server_api.update_enable_archive(True)

    def create(self):
        self.create_db_base()
        self.create_table()
        self.is_ready = True

    def get_dtable_server_api(self):
        if not self.dtable_uuid:
            return None
        return DTableServerAPI('dtable-events', self.dtable_uuid, dtable_server_url)

    def get_dtable_db_api(self):
        if not self.dtable_uuid:
            return None
        return DTableDBAPI('dtable-events', self.dtable_uuid, INNER_DTABLE_DB_URL)


class VerionBaseManager(BasicBaseManager):

    base_name = VERSION_BASE_NAME
    table_name = VERSION_TABLE_NAME

    columns = [
        {'column_name': 'version', 'column_type': 'text'}
    ]


class CDSStatisticsBaseManager(BasicBaseManager):

    base_name = CDS_STATISTICS_BASE_NAME
    table_name = CDS_STATISTICS_TABLE_NAME

    columns = [
        {'column_name': 'org_id', 'column_type': 'number'},
        {'column_name': 'sync_id', 'column_type': 'number'},
        {'column_name': 'import_or_sync', 'column_type': 'single-select'},
        {'column_name': 'sync_type', 'column_type': 'single-select'},
        {'column_name': 'started_at', 'column_type': 'date', 'column_data': {"format": "YYYY-MM-DD HH:mm", "enable_fill_default_value": False, "default_value": "", "default_date_type": "specific_date"}},
        {'column_name': 'finished_at', 'column_type': 'date', 'column_data': {"format": "YYYY-MM-DD HH:mm", "enable_fill_default_value": False, "default_value": "", "default_date_type": "specific_date"}},
        {'column_name': 'is_success', 'column_type': 'checkbox'},
        {'column_name': 'to_be_appended_rows_count', 'column_type': 'number'},
        {'column_name': 'to_be_updated_rows_count', 'column_type': 'number'},
        {'column_name': 'to_be_deleted_rows_count', 'column_type': 'number'},
        {'column_name': 'appended_rows_count', 'column_type': 'number'},
        {'column_name': 'updated_rows_count', 'column_type': 'number'},
        {'column_name': 'deleted_rows_count', 'column_type': 'number'},
        {'column_name': 'columns_count', 'column_type': 'number'},
        {'column_name': 'link_formula_columns_count', 'column_type': 'number'},
        {'column_name': 'error', 'column_type': 'long-text'}
    ]

    def create_table(self):
        super(CDSStatisticsBaseManager, self).create_table()
        dtable_server_api = self.get_dtable_server_api()
        dtable_server_api.add_column_options(self.table_name, 'import_or_sync', [
            gen_random_option('Import'),
            gen_random_option('Sync')
        ])
        dtable_server_api.add_column_options(self.table_name, 'sync_type', [
            gen_random_option('Scheduled'),
            gen_random_option('Manual')
        ])
