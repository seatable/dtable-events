from sqlalchemy import text

from dtable_events.app.config import SEATABLE_MYSQL_DB_CCNET_DB_NAME


def get_org_admins(db_session, org_id):
    sql = f"SELECT `email` FROM `{SEATABLE_MYSQL_DB_CCNET_DB_NAME}`.`OrgUser` WHERE `org_id`=:org_id AND `is_staff`=1"
    rows = db_session.execute(text(sql), {'org_id': org_id})
    admins = [row.email for row in rows]
    return admins
