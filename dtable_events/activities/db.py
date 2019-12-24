# -*- coding: utf-8 -*-
import json
import logging
from datetime import datetime, timedelta

from sqlalchemy import desc
from seaserv import ccnet_api

from dtable_events.activities.models import Activities, UserActivities

logger = logging.getLogger(__name__)


class UserActivityDetail(object):
    def __init__(self, activity):
        self.id = activity.id
        self.dtable_uuid = activity.dtable_uuid
        self.row_id = activity.row_id
        self.op_user = activity.op_user
        self.op_type = activity.op_type
        self.op_time = activity.op_time

        detail_dict = json.loads(activity.detail)
        for key in detail_dict:
            self.__dict__[key] = detail_dict[key]

    def __getitem__(self, key):
        return self.__dict__[key]


def save_or_update_or_delete(session, event):
    if event['op_type'] == 'modify_row':
        op_time = datetime.fromtimestamp(event['op_time'])
        _timestamp = op_time - timedelta(minutes=5)
        # If a table was edited many times by same user in 5 minutes, just update record.
        q = session.query(Activities)
        q = q.filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user'],
            Activities.op_time > _timestamp
        )
        row = q.first()
        if row:
            # Update cell's `value` and keep `old_value` unchanged.
            cell_old_values = dict()
            detail = json.loads(row.detail)

            for i in detail['row_data']:
                cell_old_values[i['column_key']] = i['old_value']

            for i in event['row_data']:
                if cell_old_values[i['column_key']]:
                    i['old_value'] = cell_old_values[i['column_key']]

            detail['row_data'] = event['row_data']
            detail = json.dumps(detail)
            update_user_activity_timestamp(session, row.id, op_time, detail)
        else:
            save_user_activities(session, event)
    elif event['op_type'] == 'delete_row':
        q = session.query(Activities).filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user']
        ).order_by(desc(Activities.id))
        row = q.first()
        if row and row.op_type == 'insert_row':
            session.query(Activities).filter(Activities.id == row.id).delete()
            session.query(UserActivities).filter(UserActivities.activity_id == row.id).delete()
            session.commit()
        else:
            save_user_activities(session, event)
    else:
        save_user_activities(session, event)


def update_user_activity_timestamp(session, activity_id, op_time, detail):
    activity = session.query(Activities).filter(Activities.id == activity_id)
    activity.update({"op_time": op_time, "detail": detail})
    user_activities = session.query(UserActivities).\
        filter(UserActivities.activity_id == activity_id)
    user_activities.update({"timestamp": op_time})
    session.commit()


def get_user_activities(session, username, start, limit):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    activities = list()
    try:
        q = session.query(Activities).filter(UserActivities.username == username)
        q = q.filter(UserActivities.activity_id == Activities.id)
        activities = q.order_by(desc(UserActivities.timestamp)).slice(start, start + limit).all()
    except Exception as e:
        logger.error('Get activities failed:', e)

    return [UserActivityDetail(activity) for activity in activities]


def save_user_activities(session, event):
    dtable_uuid = event['dtable_uuid']
    row_id = event['row_id']
    op_user = event['op_user']
    op_type = event['op_type']
    op_time = datetime.fromtimestamp(event['op_time'])

    table_id = event['table_id']
    table_name = event['table_name']
    row_data = event['row_data']

    detail_dict = dict()
    detail_dict["table_id"] = table_id
    detail_dict["table_name"] = table_name
    detail_dict["row_data"] = row_data
    detail = json.dumps(detail_dict)

    activity = Activities(dtable_uuid, row_id, op_user, op_type, op_time, detail)
    session.add(activity)
    session.commit()

    cmd = "SELECT to_user FROM dtable_share WHERE dtable_id=(SELECT id FROM dtables WHERE uuid=:dtable_uuid)"
    user_list = [res[r'to_user'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})]

    cmd = "SELECT owner FROM workspaces WHERE id=(SELECT workspace_id FROM dtables WHERE uuid=:dtable_uuid)"
    owner = [res[r'owner'] for res in session.execute(cmd, {"dtable_uuid": dtable_uuid})][0]

    if '@seafile_group' not in owner:
        user_list.append(op_user)
    else:
        group_id = int(owner.split('@')[0])
        members = ccnet_api.get_group_members(group_id)
        for member in members:
            if member.user_name not in user_list:
                user_list.append(member.user_name)

    for user in user_list:
        user_activity = UserActivities(activity.id, user, activity.op_time)
        session.add(user_activity)
    session.commit()
