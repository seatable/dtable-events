import argparse
import logging
import time
from datetime import datetime
from uuid import uuid4

from seaserv import seafile_api

from dtable_events.app.config import get_config, NEW_DTABLE_IN_STORAGE_SERVER, INNER_DTABLE_DB_URL, SYSTEM_BASES_OWNER
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_32_chars, gen_random_option
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.storage_backend import storage_backend

dtable_server_url = get_inner_dtable_server_url()
version_base_name = 'version'
version_table_name = 'version'

CDS_statistics_base_name = 'CDS-statistics'
CDS_statistics_table_name = 'CDS-statistics'

logging.basicConfig(level=logging.DEBUG)


def update_version(dtable_server_api: DTableServerAPI, version):
    sql = f"SELECT version FROM `{version_table_name}` LIMIT 1"
    dtable_db_api = DTableDBAPI('dtable-events', dtable_server_api.dtable_uuid, INNER_DTABLE_DB_URL)
    results, _ = dtable_db_api.query(sql, server_only=True)
    if not results:
        dtable_server_api.append_row(version_table_name, {'version': version})
    else:
        row_id = results[0]['_id']
        dtable_server_api.update_row(version_table_name, row_id, {'version': version})


def main():
    args = parser.parse_args()
    config = get_config(args.config_file)
    repo_id = seafile_api.create_repo(
        "My Workspace",
        "My Workspace",
        "dtable@seafile"
    )
    now = datetime.now()

    session_class = init_db_session_class(config)
    owner = SYSTEM_BASES_OWNER

    with session_class() as session:
        # workspace
        sql = '''
        INSERT INTO workspaces(owner, repo_id, created_at, org_id) VALUES
        (:owner, :repo_id, :created_at, -1)
        '''
        result = session.execute(sql, {
            'owner': owner,
            'repo_id': repo_id,
            'created_at': now
        })
        workspace_id = result.lastrowid

        # version base
        version_dtable_uuid = str(uuid4())
        sql = '''
        INSERT INTO dtables(uuid, name, creator, modifier, created_at, updated_at, workspace_id, in_storage) VALUES
        (:uuid, :name, :creator, modifier, :created_at, :updated_at, :workspace_id, :in_storage)
        '''
        result = session.execute(sql, {
            'uuid': uuid_str_to_32_chars(version_dtable_uuid),
            'name': version_base_name,
            'creator': owner,
            'modifier': owner,
            'created_at': now,
            'updated_at': now,
            'workspace_id': workspace_id,
            'in_storage': 1 if NEW_DTABLE_IN_STORAGE_SERVER else 0
        })
        session.commit()
        storage_backend.create_empty_dtable(version_dtable_uuid, owner, NEW_DTABLE_IN_STORAGE_SERVER, repo_id, f'{version_base_name}.dtable')
        version_dtable_server_api = DTableServerAPI('dtable-events', version_dtable_uuid, dtable_server_url)
        version_columns = [
            {'column_name': 'version', 'column_type': 'text'}
        ]
        version_dtable_server_api.add_table(version_table_name, columns=version_columns)
        try:
            version_dtable_server_api.delete_table_by_id('0000')
        except:
            pass

        # CDS-statistics base
        CDS_statistics_dtable_uuid = str(uuid4())
        sql = '''
        INSERT INTO dtables(uuid, name, creator, modifier, created_at, updated_at, workspace_id, in_storage) VALUES
        (:uuid, :name, :creator, :modifier, :created_at, :updated_at, :workspace_id, :in_storage)
        '''
        result = session.execute(sql, {
            'uuid': uuid_str_to_32_chars(CDS_statistics_dtable_uuid),
            'name': CDS_statistics_base_name,
            'creator': owner,
            'modifier': owner,
            'created_at': now,
            'updated_at': now,
            'workspace_id': workspace_id,
            'in_storage': 1 if NEW_DTABLE_IN_STORAGE_SERVER else 0
        })
        session.commit()
        storage_backend.create_empty_dtable(CDS_statistics_dtable_uuid, owner, NEW_DTABLE_IN_STORAGE_SERVER, repo_id, f'{CDS_statistics_base_name}.dtable')
        CDS_statistics_dtable_server_api = DTableServerAPI('dtable-events', CDS_statistics_dtable_uuid, dtable_server_url)
        CDS_statistic_columns = [
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
        CDS_statistics_dtable_server_api.add_table(CDS_statistics_table_name, columns=CDS_statistic_columns)
        try:
            CDS_statistics_dtable_server_api.delete_table_by_id('0000')
            CDS_statistics_dtable_server_api.add_column_options(CDS_statistics_table_name, 'import_or_sync', [
                gen_random_option('Import'),
                gen_random_option('Sync')
            ])
            CDS_statistics_dtable_server_api.add_column_options(CDS_statistics_table_name, 'sync_type', [
                gen_random_option('Scheduled'),
                gen_random_option('Manual')
            ])
        except:
            pass
        logging.info('wait for dtable-db for 5s...')
        time.sleep(5)
        update_version(version_dtable_server_api, '4.3.0')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')

    try:
        main()
    except Exception as e:
        logging.exception('upgrade system bases 4.3.0 error: %s', e)
        exit(1)
