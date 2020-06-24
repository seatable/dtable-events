# -*- coding: utf-8 -*-
import logging
import time
import json
from datetime import datetime
from threading import Thread, Event

from dtable_events.app.event_redis import redis_connection
from dtable_events.db import init_db_session_class

logger = logging.getLogger(__name__)


def count_rows_by_uuids(session, dtable_uuids):
    dtable_uuids = [uuid.replace('-', '') for uuid in dtable_uuids]
    # select user and org
    sql = '''
    SELECT w.owner, w.org_id FROM workspaces w
    JOIN dtables d ON w.id=d.workspace_id
    WHERE d.uuid in :dtable_uuids
    '''
    results = session.execute(sql, {'dtable_uuids': dtable_uuids}).fetchall()
    usernames, org_ids = set(), set()
    for owner, org_id in results:
        if org_id != -1:
            org_ids.add(org_id)
        else:
            if '@seafile_group' not in owner:
                usernames.add(owner)
    # count user and org
    # update rows_count with data from dtable_rows_count
    # and set 0 when user/org only has deleted dtables
    if usernames:
        user_sql = '''
        INSERT INTO user_rows_count(username, rows_count, rows_count_update_at)
        SELECT w.owner AS username, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count drc
        JOIN dtables d ON drc.dtable_uuid=d.uuid
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE w.owner IN :usernames AND d.deleted=0
        GROUP BY w.owner
        UNION
        SELECT w.owner AS username, 0 AS rows_count, :update_at FROM dtables d
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE w.owner IN :usernames
        GROUP BY w.owner
        HAVING COUNT(1)=SUM(d.deleted)
        ON DUPLICATE KEY UPDATE username=VALUES(username), rows_count=VALUES(rows_count), rows_count_update_at=VALUES(rows_count_update_at);
        '''
        session.execute(user_sql, {'update_at': datetime.utcnow(), 'usernames': usernames})
        session.commit()
    if org_ids:
        org_sql = '''
        INSERT INTO org_rows_count(org_id, rows_count, rows_count_update_at)
        SELECT w.org_id AS org_id, SUM(drc.rows_count) AS rows_count, :update_at FROM dtable_rows_count drc
        JOIN dtables d ON drc.dtable_uuid=d.uuid
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE w.org_id IN :org_ids AND d.deleted=0
        GROUP BY w.org_id
        UNION
        SELECT w.org_id AS org_id, 0 AS rows_count, :update_at FROM dtables d
        JOIN workspaces w ON d.workspace_id=w.id
        WHERE w.org_id IN :org_ids
        GROUP BY w.org_id
        HAVING COUNT(1)=SUM(d.deleted)
        ON DUPLICATE KEY UPDATE org_id=VALUES(org_id), rows_count=VALUES(rows_count), rows_count_update_at=VALUES(rows_count_update_at);
        '''
        session.execute(org_sql, {'update_at': datetime.utcnow(), 'org_ids': org_ids})
        session.commit()


class DTableRealTimeRowsCounter(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_connection = redis_connection(config)
        self._db_session_class = init_db_session_class(config)
        self._subscriber = self._redis_connection.pubsub(ignore_subscribe_messages=True)
        self._subscriber.subscribe('count-rows')

    def run(self):
        logger.info('Starting handle table rows count...')
        while not self._finished.is_set():
            try:
                message = self._subscriber.get_message()
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
