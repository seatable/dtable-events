# -*- coding: utf-8 -*-
import json
import time
import logging
from datetime import datetime, timedelta

from sqlalchemy import desc, func, case

from dtable_events.activities.models import Activities

logger = logging.getLogger(__name__)


class TableActivityDetail(object):
    def __init__(self, activity):
        self.id = activity.id
        self.dtable_uuid = activity.dtable_uuid
        self.row_id = activity.row_id
        self.row_count = activity.row_count
        self.op_user = activity.op_user
        self.op_type = activity.op_type
        self.op_time = activity.op_time
        self.op_app = activity.op_app

        detail_dict = json.loads(activity.detail)
        for key in detail_dict:
            self.__dict__[key] = detail_dict[key]

    def __getitem__(self, key):
        return self.__dict__[key]


def save_or_update_or_delete(session, event):
    # ignore a few cloumn data: creator, ctime, last-modifier, mtime
    for cell_data in event['row_data']:
        if cell_data.get('column_type', '') in ['creator', 'ctime', 'last-modifier', 'mtime']:
            event['row_data'].remove(cell_data)

    if event['op_type'] == 'modify_row':
        op_time = datetime.utcfromtimestamp(event['op_time'])
        _timestamp = op_time - timedelta(minutes=5)
        # If a row was edited many times by same user in 5 minutes, just update record.
        q = session.query(Activities).filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user'],
            Activities.op_time > _timestamp
        ).order_by(desc(Activities.id))
        row = q.first()
        if row:
            if row.op_type == 'insert_row':
                detail = json.loads(row.detail)
                cells_data = event['row_data']
                # Update cells values.
                for cell_data in cells_data:
                    for i in detail['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            if i['column_type'] != cell_data['column_type']:
                                i['column_type'] = cell_data['column_type']
                                i['column_data'] = cell_data['column_data']
                            break
                    else:
                        del cell_data['old_value']
                        detail['row_data'].append(cell_data)
                detail['row_name'] = event['row_name']
                detail['row_name_option'] = event.get('row_name_option', '')
            else:
                detail = json.loads(row.detail)
                cells_data = event['row_data']
                # Update cells values and keep old_values unchanged.
                for cell_data in cells_data:
                    for i in detail['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            if i['column_type'] != cell_data['column_type']:
                                i['column_type'] = cell_data['column_type']
                                i['column_data'] = cell_data['column_data']
                                i['old_value'] = cell_data['old_value']
                            break
                    else:
                        detail['row_data'].append(cell_data)
                detail['row_name'] = event['row_name']
                detail['row_name_option'] = event.get('row_name_option', '')

            detail = json.dumps(detail)
            update_activity_timestamp(session, row.id, op_time, detail)
        else:
            save_user_activities(session, event)
    elif event['op_type'] == 'delete_row':
        op_time = datetime.utcfromtimestamp(event['op_time'])
        _timestamp = op_time - timedelta(minutes=5)
        # If a row was inserted by same user in 5 minutes, just delete this record.
        q = session.query(Activities).filter(
            Activities.row_id == event['row_id'],
            Activities.op_user == event['op_user'],
            Activities.op_time > _timestamp
        ).order_by(desc(Activities.id))
        row = q.first()
        if row and row.op_type == 'insert_row':
            session.query(Activities).filter(Activities.id == row.id).delete()
            session.commit()
        else:
            save_user_activities(session, event)
    else:
        save_user_activities(session, event)


def update_activity_timestamp(session, activity_id, op_time, detail):
    activity = session.query(Activities).filter(Activities.id == activity_id)
    activity.update({"op_time": op_time, "detail": detail})
    session.commit()


def get_table_activities(session, uuid_list, start, limit, to_tz):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    table_activities = list()
    try:
        q = session.query(
            Activities.dtable_uuid, Activities.op_time.label('op_date'),
            func.date_format(func.convert_tz(Activities.op_time, '+00:00', to_tz), '%Y-%m-%d 00:00:00').label('date'),
            func.sum(case([(Activities.op_type == 'insert_row', Activities.row_count)])).label('insert_row'),
            func.sum(case([(Activities.op_type == 'modify_row', Activities.row_count)])).label('modify_row'),
            func.sum(case([(Activities.op_type == 'delete_row', Activities.row_count)])).label('delete_row'))
        q = q.filter(
            Activities.op_time > (datetime.utcnow() - timedelta(days=7)),
            Activities.dtable_uuid.in_(uuid_list)).group_by(Activities.dtable_uuid, 'date')
        table_activities = q.order_by(desc(Activities.op_time)).slice(start, start + limit).all()
    except Exception as e:
        logger.error('Get table activities failed: %s' % e)

    return table_activities


def get_activities_detail(session, dtable_uuid, start_time, end_time, start, limit, to_tz):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if limit <= 0:
        logger.error('limit must be positive')
        raise RuntimeError('limit must be positive')

    activities = list()
    try:
        q = session.query(Activities).filter(Activities.dtable_uuid == dtable_uuid).\
            filter(func.convert_tz(Activities.op_time, '+00:00', to_tz).between(start_time, end_time))
        activities = q.order_by(desc(Activities.op_time)).slice(start, start + limit).all()
    except Exception as e:
        logger.error('Get table activities detail failed: %s' % e)

    activities_detail = list()
    for activity in activities:
        try:
            activity_detail = TableActivityDetail(activity)
            activities_detail.append(activity_detail)
        except Exception as e:
            logger.warning(e)
            continue

    return activities_detail


def save_user_activities(session, event):
    dtable_uuid = event['dtable_uuid']
    row_id = event['row_id']
    op_user = event['op_user']
    op_type = event['op_type']
    op_time = datetime.utcfromtimestamp(event['op_time'])
    op_app = event.get('op_app')

    table_id = event['table_id']
    table_name = event['table_name']
    row_name = event['row_name']
    row_name_option = event.get('row_name_option', '')
    row_data = event['row_data']
    row_count = event.get('row_count', 1)

    detail_dict = dict()
    detail_dict["table_id"] = table_id
    detail_dict["table_name"] = table_name
    detail_dict["row_name"] = row_name
    detail_dict["row_name_option"] = row_name_option
    detail_dict["row_data"] = row_data
    detail = json.dumps(detail_dict)

    activity = Activities(dtable_uuid, row_id, row_count, op_user, op_type, op_time, detail, op_app)
    session.add(activity)
    session.commit()
