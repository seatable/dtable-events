import logging

from dtable_events.app.config import SYSTEM_BASES_OWNER
from dtable_events.system_bases.constants import VERSION_BASE_NAME, VERSION_TABLE_NAME


def get_base_by_name(session, name):
    sql = '''
    SELECT uuid FROM dtables d
    JOIN workspaces w ON d.workspace_id=w.id
    WHERE w.owner=:owner AND d.name=:name
    '''
    result = session.execute(sql, {'owner': SYSTEM_BASES_OWNER, 'name': name}).fetch_one()
    if not result:
        return None
    else:
        return result


def create_base_by_name(session, name):
    sql = '''
    SELECT workspace_id FROM workspaces WHERE owner=:owner
    '''
    result = session.execute(session, {'owner': SYSTEM_BASES_OWNER}).fetch_one()
    if not result:
        logging.error('system workspace not found!')


def check_or_create_version(session):
    # check base
    base = get_base_by_name(session, VERSION_BASE_NAME)
    # check table
