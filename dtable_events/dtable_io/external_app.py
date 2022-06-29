from datetime import datetime

from seatable_api.constants import ColumnTypes
from dateutil.parser import parse

APP_USERS_COUMNS_TYPE_MAP = {
    "Name" : ColumnTypes.TEXT,
    "User": ColumnTypes.COLLABORATOR,
    "Role": ColumnTypes.TEXT,
    # "RolePermission": ColumnTypes.TEXT,
    "IsActive": ColumnTypes.CHECKBOX,
    "JoinedAt": ColumnTypes.TEXT,
}


def get_row_info_by_app_username(rows, username):
    for row in rows:
        if row.get('User') == [username, ]:
            return row
    return None

def parse_dt_str(dt_str):
    dt_obj = parse(dt_str)
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S")


def match_user_info(rows, username, user_info):
    row_info = get_row_info_by_app_username(rows, username)
    if not row_info:
        return False, 'create', None

    name = row_info.get('Name')
    role_name = row_info.get('Role')
    is_active = row_info.get('IsActive')


    if user_info.get('name', '') == name and \
        user_info.get('role_name') == role_name and \
        user_info.get('is_active') == is_active:
        return True, None, None
    return False, 'update', row_info.get('_id')

def update_app_syncer(db_session, app_id, table_id):
    sql = """
    INSERT INTO dtable_app_user_syncer (app_id, dst_table_id, created_at, updated_at) VALUES
    (:app_id, :dst_table_id, :created_at, :updated_at)
    ON DUPLICATE KEY UPDATE
    updated_at=:updated_at,
    dst_table_id=:dst_table_id
    """

    db_session.execute(sql, {
        'app_id': app_id,
        'dst_table_id': table_id,
        'created_at': datetime.utcnow(),
        'updated_at': datetime.utcnow(),
    })

    db_session.commit()
