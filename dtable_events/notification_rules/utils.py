import logging

from sqlalchemy import text

from dtable_events.app.event_redis import redis_cache as cache

logger = logging.getLogger(__name__)


def get_nickname_by_usernames(usernames, db_session):
    """
    fetch nicknames by usernames from db / cache
    return: {username0: nickname0, username1: nickname1...}
    """
    if not usernames:
        return {}
    cache_timeout = 60*60*24
    key_format = 'user:nickname:%s'
    users_dict, miss_users = {}, []

    for username in usernames:
        nickname = cache.get(key_format % username)
        if nickname is None:
            miss_users.append(username)
        else:
            users_dict[username] = nickname
            cache.set(key_format % username, nickname, timeout=cache_timeout)

    if not miss_users:
        return users_dict

    # miss_users is not empty
    sql = "SELECT user, nickname FROM profile_profile WHERE user in :users"
    try:
        for username, nickname in db_session.execute(text(sql), {'users': usernames}).fetchall():
            users_dict[username] = nickname
            cache.set(key_format % username, nickname, timeout=cache_timeout)
    except Exception as e:
        logger.error('check nicknames error: %s', e)

    return users_dict


def get_department_name(department_id, db_session):
    cache_timeout = 60 * 60 * 24
    cache_key = f'department:name:{department_id}'
    department_name = cache.get(cache_key)
    if department_name:
        cache.set(cache_key, department_name, cache_timeout)
        return department_name
    sql = "SELECT name FROM departments_v2 WHERE id=:department_id"
    try:
        department = db_session.execute(sql, {'department_id': department_id}).fetchone()
    except Exception as e:
        logger.error('check department: %s name error: %s', department_id, e)
        return ''
    if not department:
        return ''
    cache.set(cache_key, department.name, cache_timeout)
    return department.name
