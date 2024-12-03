# -*- coding: utf-8 -*-
import json
import logging
import re
from datetime import datetime, timedelta

import pytz
from sqlalchemy import select, update, delete, desc, func, case, text

from dtable_events.activities.models import Activities
from dtable_events.app.config import TIME_ZONE

logger = logging.getLogger(__name__)

ROWS_OPERATION_TYPES = [
        'insert_row',
        'insert_rows',
        'append_rows',
        'delete_row',
        'delete_rows',
        'modify_row',
        'modify_rows',
    ]

LINK_OPERATION_TYPES = [
    'add_link',
    'remove_link',
    'update_links',
    'update_rows_links'
]

DETAIL_LIMIT = 65535  # 2^16 - 1


class TableActivity(object):
    pass


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
    # ignore a few column data: creator, ctime, last-modifier, mtime

    if event['op_type'] in ROWS_OPERATION_TYPES:
        for cell_data in event['row_data']:
            if cell_data.get('column_type', '') in ['creator', 'ctime', 'last-modifier', 'mtime']:
                event['row_data'].remove(cell_data)

        if event['op_type'] == 'modify_row':
            op_time = datetime.utcfromtimestamp(event['op_time'])
            _timestamp = op_time - timedelta(minutes=5)
            # If a row was edited many times by same user in 5 minutes, just update record.
            stmt = select(Activities).where(
                Activities.row_id == event['row_id'],
                Activities.op_user == event['op_user'],
                Activities.op_time > _timestamp
            ).order_by(desc(Activities.id)).limit(1)
            row = session.scalars(stmt).first()
            if row:
                detail = json.loads(row.detail)
                if detail['table_id'] == event['table_id']:
                    if row.op_type == 'insert_row':
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
            else:
                save_user_activities(session, event)
        elif event['op_type'] == 'delete_row':
            op_time = datetime.utcfromtimestamp(event['op_time'])
            _timestamp = op_time - timedelta(minutes=5)
            # If a row was inserted by same user in 5 minutes, just delete this record.
            stmt = select(Activities).where(
                Activities.row_id == event['row_id'],
                Activities.op_user == event['op_user'],
                Activities.op_time > _timestamp
            ).order_by(desc(Activities.id)).limit(1)
            row = session.scalars(stmt).first()
            if row and row.op_type == 'insert_row':
                session.execute(delete(Activities).where(Activities.id == row.id))
                session.commit()
            else:
                save_user_activities(session, event)
        else:
            save_user_activities(session, event)
    else:  # handle link   
        if event['op_type'] in LINK_OPERATION_TYPES:
            op_time = datetime.utcfromtimestamp(event['op_time'])
            _timestamp = op_time - timedelta(minutes=5)
            # If a row was edited many times by same user in 5 minutes, just update record.
            stmt1 = select(Activities).where(
                Activities.row_id == event['table1_row_id'],
                Activities.op_user == event['op_user'],
                Activities.op_time > _timestamp
            ).order_by(desc(Activities.id)).limit(1)
            stmt2 = select(Activities).where(
                Activities.row_id == event['table2_row_id'],
                Activities.op_user == event['op_user'],
                Activities.op_time > _timestamp
            ).order_by(desc(Activities.id)).limit(1)
            row1 = session.scalars(stmt1).first()
            row2 = session.scalars(stmt2).first()
            if row1:
                detail1 = json.loads(row1.detail)
                cells_data1 = event['row_data1']
                for cell_data in cells_data1:
                    for i in detail1['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            break
                    else:
                        detail1['row_data'].append(cell_data)
                detail1['row_name'] = event['table1_row_name']
                detail1['row_name_option'] = event.get('row_name_option', '')

                detail1 = json.dumps(detail1)
                update_link_activity_timestamp(session, row1.id, op_time, detail1, op_type='modify_row')
            
            if row2:
                detail2 = json.loads(row2.detail)
                cells_data2 = event['row_data2']
                for cell_data in cells_data2:
                    for i in detail2['row_data']:
                        if i['column_key'] == cell_data['column_key']:
                            i['value'] = cell_data['value']
                            break
                    else:
                        detail2['row_data'].append(cell_data)
                detail2['row_name'] = event['table2_row_name']
                detail2['row_name_option'] = event.get('row_name_option', '')

                detail2 = json.dumps(detail2)
                update_link_activity_timestamp(session, row2.id, op_time, detail2, op_type='modify_row')

            save_user_activities_by_link(session, event, row1, row2)


def update_activity_timestamp(session, activity_id, op_time, detail):
    if len(detail) > DETAIL_LIMIT:
        return
    stmt = update(Activities).where(Activities.id == activity_id).values({"op_time": op_time, "detail": detail})
    session.execute(stmt)
    session.commit()


def update_link_activity_timestamp(session, activity_id, op_time, detail, op_type=None):
    if len(detail) > DETAIL_LIMIT:
        return
    stmt = update(Activities).where(Activities.id == activity_id).values({"op_time": op_time, "detail": detail})
    if op_type:
        stmt = update(Activities).where(Activities.id == activity_id).values(
            {"op_time": op_time, "detail": detail, 'op_type': op_type})
    session.execute(stmt)
    session.commit()


def get_shifted_days_ago(offset_str, days):
    """Convert utcnow to offset time, then return n days ago 00:00:00 time in TIME_ZONE config item timezone
    The reason why convert to TIME_ZONE because `updated_at` in `dtables` is in TIME_ZONE

    Eg:
        offset_str: +5:00
        days: 3
        UTC now: 2024-11-06 11:00:00
        TIME_ZONE: Asia/Shanghai (+8:00)
        return: 
            2024-11-06 11:00:00 (utc) -> utc to offset timezone
            2024-11-06 16:00:00 (+5:00) -> 3 days ago
            2024-11-03 16:00:00 (+5:00) -> 00:00:00
            2024-11-03 00:00:00 (+5:00) -> to TIME_ZONE
            2024-11-03 03:00:00 (+8:00)
    """

    match = re.match(r'([+-])(\d{1,2}):(\d{2})', offset_str)
    if not match:
        raise ValueError("Offset format must be like '+8:00' or '-9:00'")

    sign = 1 if match.group(1) == '+' else -1
    hours = int(match.group(2)) * sign

    gmt_offset = f"Etc/GMT{'-' if hours >= 0 else '+'}{abs(hours)}"

    utc_now = datetime.utcnow().replace(tzinfo=pytz.utc)
    target_timezone = pytz.timezone(gmt_offset)
    local_time = utc_now.astimezone(target_timezone)

    days_ago_start = (local_time - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

    days_ago_start = days_ago_start.astimezone(pytz.timezone(TIME_ZONE))

    return days_ago_start


def filter_user_activate_tables(session, days, uuid_list, to_tz):
    start_str = get_shifted_days_ago(to_tz, days).strftime('%Y-%m-%d %H:%M:%S')
    # filter dtable_uuids
    sql = "SELECT uuid FROM dtables WHERE uuid IN :dtable_uuids AND updated_at > :start_time"
    dtable_uuids = []
    for row in session.execute(text(sql), {'dtable_uuids': uuid_list, 'start_time': start_str}):
        dtable_uuids.append(row.uuid)
    return dtable_uuids


def get_table_activities(session, uuid_list, days, start, limit, to_tz):
    if not uuid_list:
        return []

    activities = list()
    try:
        uuid_list = filter_user_activate_tables(session, days, uuid_list, to_tz)
        if not uuid_list:
            return []
        start_utc_str = get_shifted_days_ago(to_tz, days).astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        # query activities
        stmt = select(
            Activities.dtable_uuid, Activities.op_time.label('op_date'),
            func.date_format(func.convert_tz(Activities.op_time, '+00:00', to_tz), '%Y-%m-%d 00:00:00').label('date'),
            func.sum(case((Activities.op_type == 'insert_row', Activities.row_count))).label('insert_row'),
            func.sum(case((Activities.op_type == 'modify_row', Activities.row_count))).label('modify_row'),
            func.sum(case((Activities.op_type == 'delete_row', Activities.row_count))).label('delete_row')
        ).where(
            Activities.op_time > start_utc_str, Activities.dtable_uuid.in_(uuid_list)
        ).group_by(Activities.dtable_uuid, 'date').order_by(desc(Activities.op_time)).slice(start, start + limit)
        activities = session.execute(stmt).all()
    except Exception as e:
        logger.exception('Get table activities failed: %s', e)

    table_activities = list()
    for dtable_uuid, op_date, date, insert_row, modify_row, delete_row in activities:
        table_activity = TableActivity()
        table_activity.dtable_uuid = dtable_uuid
        table_activity.op_date = op_date
        table_activity.date = date
        table_activity.insert_row = insert_row
        table_activity.modify_row = modify_row
        table_activity.delete_row = delete_row
        table_activities.append(table_activity)

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
        stmt = select(Activities).where(
            Activities.dtable_uuid == dtable_uuid,
            func.convert_tz(Activities.op_time, '+00:00', to_tz).between(start_time, end_time)
        ).order_by(desc(Activities.id)).slice(start, start + limit)
        activities = session.scalars(stmt).all()
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
    if len(detail) > DETAIL_LIMIT:
        return

    activity = Activities(dtable_uuid, row_id, row_count, op_user, op_type, op_time, detail, op_app)
    session.add(activity)
    session.commit()


def save_user_activities_by_link(session, event, row1, row2):
    if row1 and row2:
        return
    
    dtable_uuid = event['dtable_uuid']
    
    op_user = event['op_user']
    op_type = 'modify_row'
    op_time = datetime.utcfromtimestamp(event['op_time'])
    op_app = event.get('op_app')

    row_id1 = event['table1_row_id']
    table_id1 = event['table1_id']
    table_name1 = event['table1_name']
    row_name1 = event['table1_row_name']
    row_name_option = event.get('row_name_option', '')
    row_data1 = event['row_data1']
    row_count1 = event.get('row_count', 1)

    row_id2 = event['table2_row_id']
    table_id2 = event['table2_id']
    table_name2 = event['table2_name']
    row_name2 = event['table2_row_name']
    row_data2 = event['row_data2']
    row_count2 = event.get('row_count', 1)

    detail_dict1, detail_dict2 = dict(), dict()
    detail_dict1["table_id"] = table_id1
    detail_dict1["table_name"] = table_name1
    detail_dict1["row_name"] = row_name1
    detail_dict1["row_name_option"] = row_name_option
    detail_dict1["row_data"] = row_data1
    detail1 = json.dumps(detail_dict1)

    detail_dict2["table_id"] = table_id2
    detail_dict2["table_name"] = table_name2
    detail_dict2["row_name"] = row_name2
    detail_dict2["row_name_option"] = row_name_option
    detail_dict2["row_data"] = row_data2
    detail2 = json.dumps(detail_dict2)
    if (not row1) and len(detail1) <= DETAIL_LIMIT:
        activity1 = Activities(dtable_uuid, row_id1, row_count1, op_user, op_type, op_time, detail1, op_app)
        session.add(activity1)
    if (not row2) and len(detail2) <= DETAIL_LIMIT:
        activity2 = Activities(dtable_uuid, row_id2, row_count2, op_user, op_type, op_time, detail2, op_app)
        session.add(activity2)
    session.commit()

def cache_dtable_update_info(app, event):
    cache = app.dtable_update_cache
    dtable_uuid = event.get('dtable_uuid')
    op_time = int(event.get('op_time'))
    cache.set_update_time(dtable_uuid=dtable_uuid, op_time=op_time)
