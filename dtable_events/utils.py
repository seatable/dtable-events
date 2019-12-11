# -*- coding: utf-8 -*-
import json
import logging

from sqlalchemy import desc

from dtable_events.models import Activities, UserActivities

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
        activities = q.order_by(desc(UserActivities.id)).slice(start, start + limit).all()
    except Exception as e:
        logger.error(e)

    return [UserActivityDetail(activity) for activity in activities]
