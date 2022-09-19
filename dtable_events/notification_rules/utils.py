import logging

from dtable_events.cache import redis_cache as cache

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
        for username, nickname in db_session.execute(sql, {'users': usernames}).fetchall():
            users_dict[username] = nickname
            cache.set(key_format % username, nickname, timeout=cache_timeout)
    except Exception as e:
        logger.error('check nicknames error: %s', e)

    return users_dict
