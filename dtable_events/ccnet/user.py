from sqlalchemy import text

from dtable_events.app.config import CCNET_DB_NAME


def get_user_role(db_session, username):
    sql = f"SELECT `role` FROM `{CCNET_DB_NAME}`.`UserRole` WHERE `email`=:email"
    row = db_session.execute(text(sql), {'email': username}).fetchone()
    return getattr(row, 'role', )
