from datetime import datetime

from seatable_api.constants import ColumnTypes

APP_USERS_COUMNS_TYPE_MAP = {
    "UserID" : ColumnTypes.TEXT,
    "Username": ColumnTypes.COLLABORATOR,
    "RoleName": ColumnTypes.TEXT,
    "RolePermission": ColumnTypes.TEXT,
    "IsActive": ColumnTypes.CHECKBOX,
}


def get_row_info_by_app_username(rows, username):
    for row in rows:
        if row.get('Username') == [username, ]:
            return row
    return None

def match_user_info(rows, username, user_info):
    row_info = get_row_info_by_app_username(rows, username)
    if not row_info:
        return False, 'create', None

    user_id = row_info.get('UserID')
    role_name = row_info.get('RoleName')
    role_permission = row_info.get('RolePermission')
    is_active = row_info.get('IsActive')


    if str(user_info.get('id', '')) == user_id and \
        user_info.get('role_name') == role_name and \
        user_info.get('role_permission') == role_permission and \
        user_info.get('is_active') == is_active:
        return True, None, None
    return False, 'update', row_info.get('_id')

def update_app_syncer(db_session, app_id, table_id):
    sql = """
    INSERT INTO dtable_app_user_syncer (app_id, dst_table_id, sync_count, created_at, updated_at) VALUES
    (:app_id, :dst_table_id, 1, :created_at, :updated_at)
    ON DUPLICATE KEY UPDATE
    sync_count=sync_count+1,
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