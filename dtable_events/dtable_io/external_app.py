from datetime import datetime
from dateutil.parser import parse
from dtable_events.utils.constants import ColumnTypes

APP_USERS_COUMNS_TYPE_MAP = {
    "Name" : ColumnTypes.TEXT,
    "User": ColumnTypes.COLLABORATOR,
    "Role": ColumnTypes.TEXT,
    # "RolePermission": ColumnTypes.TEXT,
    "IsActive": ColumnTypes.CHECKBOX,
    "JoinedAt": ColumnTypes.TEXT,
}

def get_row_ids_for_delete(rows_name_id_map, userlist):
    username_list = [user.get('email') for user in userlist]
    username_for_delete = set(rows_name_id_map.keys()).difference(set(username_list))
    return [rows_name_id_map.get(username, {}).get('_id') for username in username_for_delete]

def match_user_info(rows_name_id_map, username, user_info):
    row_info = rows_name_id_map.get(username, None)
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

def update_app_sync(db_session, app_id, table_id):
    sql = """
    INSERT INTO dtable_app_user_sync (app_id, dst_table_id, created_at, updated_at) VALUES
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

def get_app_users(db_session, app_id):
    start, offset, user_list = 0, 1000, []
    count_sql = "SELECT COUNT(1) AS count FROM dtable_app_users where app_id=:app_id"
    count_result = db_session.execute(count_sql, {'app_id': app_id})
    total_count = count_result.cursor.fetchone()[0]
    while start <= total_count:
        sql = """
        SELECT u.username, p.nickname, r.role_name, u.is_active, u.created_at
        FROM
        dtable_app_users AS u
        LEFT JOIN profile_profile p ON u.username = p.user
        LEFT JOIN dtable_app_roles r ON u.role_id = r.id
        WHERE u.app_id=:app_id
        LIMIT :start, :offset
        """
        results = db_session.execute(sql, {'app_id': app_id, 'start': start, 'offset': offset})
        users = []
        for username, nickname, role_name, is_active, created_at in results:
            users.append({
                'email': username,
                'name': nickname,
                'role_name': role_name,
                'is_active': is_active,
                'created_at': created_at.strftime("%Y-%m-%d %H:%M:%S"),
            })
        user_list.extend(users)
        start += offset

    return user_list
