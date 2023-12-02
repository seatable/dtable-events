# -*- coding: utf-8 -*-
import logging
import time
import json
from datetime import datetime
from threading import Thread, Event

from sqlalchemy import text

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def count_rows_by_uuids(session, dtable_uuids):
    dtable_uuids = [uuid.replace('-', '') for uuid in dtable_uuids]
    # select user and org
    sql = '''
    SELECT owner, org_id FROM dtable_rows_count
    WHERE dtable_uuid IN :dtable_uuids
    '''
    results = session.execute(text(sql), {'dtable_uuids': dtable_uuids}).fetchall()
    usernames, org_ids = set(), set()
    for owner, org_id in results:
        if org_id != -1:
            org_ids.add(org_id)
        else:
            if '@seafile_group' not in owner:
                usernames.add(owner)
    usernames, org_ids = list(usernames), list(org_ids)
    # count user and org
    if usernames:
        step = 1000
        for i in range(0, len(usernames), step):
            sub_usernames = usernames[i: i+step]
            # query and update
            ## query
            query_sql = '''
            SELECT drc.owner AS username, SUM(drc.rows_count) AS rows_count FROM dtable_rows_count drc
            JOIN dtables d ON drc.dtable_uuid=d.uuid
            WHERE drc.owner IN :usernames AND d.deleted=0
            GROUP BY drc.owner
            '''
            now = datetime.now()
            try:
                results = session.execute(text(query_sql), {
                    'usernames': list(sub_usernames)
                }).fetchall()
            except Exception as e:
                logger.error('query users rows error: %s', e)
            else:
                ## update
                user_counts, user_set = [], set()
                for result in results:
                    user_counts.append((result[0], result[1], ':update_at'))
                    user_set.add(result[0])
                for user in sub_usernames:
                    if user in user_set:
                        continue
                    # user who has no dtables deleted=False
                    user_counts.append((user, 0, ':update_at'))
                if user_counts:
                    update_sql = '''
                    INSERT INTO user_rows_count(username, rows_count, rows_count_update_at) VALUES %s
                    ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
                    ''' % ', '.join(["('%s', %s, %s)" % user_count for user_count in user_counts])
                    try:
                        session.execute(text(update_sql), {'update_at': now})
                        session.commit()
                    except Exception as e:
                        logger.error('update users rows error: %s', e)

    if org_ids:
        step = 1000
        for i in range(0, len(org_ids), step):
            sub_org_ids = org_ids[i: i+step]
            # query and update
            ## query
            query_sql = '''
            SELECT drc.org_id, SUM(drc.rows_count) AS rows_count FROM dtable_rows_count as drc
            JOIN dtables d ON drc.dtable_uuid=d.uuid
            WHERE drc.org_id IN :org_ids AND d.deleted=0
            GROUP BY drc.org_id
            '''
            now = datetime.now()
            try:
                results = session.execute(text(query_sql), {
                    'org_ids': list(sub_org_ids)
                })
            except Exception as e:
                logger.error('query orgs rows error: %s', e)
            else:
                ## update
                org_counts, org_id_set = [], set()
                for result in results:
                    org_counts.append((result[0], result[1], ':update_at'))
                    org_id_set.add(result[0])
                for org_id in sub_org_ids:
                    if org_id in org_id_set:
                        continue
                    # org who has no dtables deleted=False
                    org_counts.append((org_id, 0, ':update_at'))
                if org_counts:
                    update_sql = '''
                    INSERT INTO org_rows_count(org_id, rows_count, rows_count_update_at) VALUES %s
                    ON DUPLICATE KEY UPDATE rows_count=VALUES(rows_count), rows_count_update_at=:update_at;
                    ''' % ', '.join(["(%s, %s, %s)" % org_count for org_count in org_counts])
                    try:
                        session.execute(text(update_sql), {'update_at': now})
                        session.commit()
                    except Exception as e:
                        logger.error('update users rows error: %s', e)


class DTableRealTimeRowsCounter(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)


    def run(self):
        logger.info('Starting handle table rows count...')
        subscriber = self._redis_client.get_subscriber('count-rows')
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    dtable_uuids = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        count_rows_by_uuids(session, dtable_uuids)
                    except Exception as e:
                        logger.error('Handle table rows count: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('count-rows')
