import argparse
import logging
from datetime import datetime
from uuid import uuid4

from seaserv import seafile_api

from dtable_events.app.config import get_config, NEW_DTABLE_IN_STORAGE_SERVER, INNER_DTABLE_DB_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_32_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.storage_backend import storage_backend

dtable_server_url = get_inner_dtable_server_url()
version_table = 'version'


def update_version(dtable_server_api: DTableServerAPI, version):
    sql = f"SELECT version FROM {version_table} LIMIT 1"
    dtable_db_api = DTableDBAPI('dtable-events', dtable_server_api.dtable_uuid, INNER_DTABLE_DB_URL)
    results, _ = dtable_db_api.query(sql, server_only=True)
    if not results:
        dtable_server_api.append_row(version_table, {'version': version})
    else:
        row_id = results[0]['_id']
        dtable_server_api.update_row(version_table, row_id, {'version': version})


def main():
    args = parser.parse_args()
    config = get_config(args.config_file)
    repo_id = seafile_api.create_repo(
        "My Workspace",
        "My Workspace",
        "dtable@seafile"
    )
    now = datetime.now()
    base_name = 'version'
    dtable_uuid = str(uuid4())

    session_class = init_db_session_class(config)
    owner = 'system base'

    with session_class() as session:
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
        sql = '''
        INSERT INTO dtables(uuid, name, creator, modifier, created_at, updated_at, workspace_id, in_storage) VALUES
        (:uuid, :name, :creator, modifier, :created_at, :updated_at, :workspace_id, :in_storage)
        '''
        result = session.execute(sql, {
            'uuid': uuid_str_to_32_chars(dtable_uuid),
            'name': base_name,
            'creator': owner,
            'modifier': owner,
            'created_at': now,
            'updated_at': now,
            'workspace_id': workspace_id,
            'in_storage': 1 if NEW_DTABLE_IN_STORAGE_SERVER else 0
        })
    storage_backend.create_empty_dtable(dtable_uuid, owner, NEW_DTABLE_IN_STORAGE_SERVER, repo_id, f'{base_name}.dtable')
    dtable_server_api = DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)
    try:
        dtable_server_api.delete_table_by_id('0000')
    except:
        pass
    columns = [
        {'name': 'version', 'type': 'text'}
    ]
    dtable_server_api.add_table(version_table, columns)
    update_version(dtable_server_api, '0.0.1')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')

    try:
        main()
    except Exception as e:
        logging.exception('upgrade system bases 0.0.1 error: %s', e)
        exit(1)
