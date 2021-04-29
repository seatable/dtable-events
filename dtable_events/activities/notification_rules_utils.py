# -*- coding: utf-8 -*-
import logging
import time
import json
import os
from datetime import datetime, date, timedelta
import requests
import jwt
import sys
import re
import pytz

from dtable_events.utils.constants import ColumnTypes

logger = logging.getLogger(__name__)


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

sys.path.insert(0, dtable_web_dir)
try:
    from seahub.settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)


CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'



def is_trigger_time_satisfy(last_trigger_time):
    if last_trigger_time == None:
        return True
    if (datetime.utcnow() - last_trigger_time).total_seconds() >= 60 * 5:
        return True


def update_rule_last_trigger_time(rule_id, db_session):

    cmd = "UPDATE dtable_notification_rules SET last_trigger_time=:new_time WHERE id=:rule_id"
    db_session.execute(cmd, {'new_time': datetime.utcnow(), 'rule_id': rule_id})


def get_dtable_server_token(dtable_uuid):
    payload = {
        'exp': int(time.time()) + 60,
        'dtable_uuid': dtable_uuid,
        'username': 'dtable-web',
        'permission': 'rw',
    }
    try:
        access_token = jwt.encode(
            payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
        )
    except Exception as e:
        logger.error(e)
        return
    return access_token


def scan_notifications_rules_per_update(event_data, db_session):
    row_id = event_data.get('row_id', '')
    message_dtable_uuid = event_data.get('dtable_uuid', '')
    table_id = event_data.get('table_id', '')
    row_data = event_data.get('row_data', [])
    column_keys = [cell.get('column_key') for cell in row_data if 'column_key' in cell]
    if not row_id or not message_dtable_uuid or not table_id:
        logger.error(f'redis event data not valid, event_data = {event_data}')
        return

    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules WHERE run_condition='per_update'" \
          "AND dtable_uuid=:dtable_uuid"
    rules = db_session.execute(sql, {'dtable_uuid': message_dtable_uuid})

    dtable_server_access_token = get_dtable_server_token(message_dtable_uuid)

    for rule in rules:
        try:
            check_notification_rule(rule, table_id, row_id, column_keys, dtable_server_access_token, db_session)
        except Exception as e:
            logger.error(f'check rule failed. {rule}, error: {e}')
    db_session.commit()


def list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, column_key, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/' + row_id + '/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {
        'table_id': table_id,
        'view_id': view_id,
        'convert': False,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        logger.error(f'failed to list_users_by_column_key {res.text}')

    rowdict = json.loads(res.content)
    user_list = rowdict.get(column_key, [])
    if isinstance(user_list, str):
        return [user_list]
    return user_list


def send_notification(dtable_uuid, user_msg_list, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/notifications-batch/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    body = {
        'user_messages': user_msg_list,
    }
    res = requests.post(url, headers=headers, json=body)

    if res.status_code != 200:
        logger.error(f'failed to send_notification {res.text}')


def is_row_in_view(row_id, view_id, dtable_uuid, table_id, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-in-view/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {
        'row_id': row_id,
        'view_id': view_id,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_in_view')


def is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/tables/' + table_id + '/is-row-satisfy-filters/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    data = {
        'row_id': row_id,
        'filters': filters,
        'filter_conjunction': filter_conjuntion
    }
    res = requests.get(url, headers=headers, json=data)

    if res.status_code != 200:
        logger.error(res.text)
        return False
    return json.loads(res.content).get('is_row_satisfy_filters')


def list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    query_params = {
        'table_id': table_id,
        'view_id': view_id,
        'convert_link_id': True
    }
    try:
        res = requests.get(url, headers=headers, params=query_params)
    except Exception as e:
        logger.error(e)
        return []

    if res.status_code != 200:
        logger.error(res.text)
        return []

    rows = json.loads(res.content).get('rows', [])
    rows_near_deadline = []
    for row in rows:
        deadline_date_date_str = row.get(date_column_name, '')
        if not deadline_date_date_str:
            continue
        if ' ' in deadline_date_date_str:
            deadline_date_date_str = deadline_date_date_str.split(' ')[0]
        try:
            deadline_date = datetime.strptime(deadline_date_date_str, '%Y-%m-%d').date()
        except Exception as e:
            # perhaps result-type of fomular column has been changed to non-date
            logger.warning('date_column_name: %s value: %s, transfer to date error: %s', date_column_name, deadline_date_date_str, e)
            continue
        now_plus_alarm_date = date.today() + timedelta(days=int(alarm_days))
        if date.today() <= deadline_date <= now_plus_alarm_date:
            rows_near_deadline.append(row)
    return rows_near_deadline


def get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/columns/'
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    query_params = {
        'table_id': table_id,
        'view_id': view_id
    }
    try:
        response = requests.get(url, params=query_params, headers=headers)
        columns = response.json()['columns']
    except Exception as e:
        logger.error('dtable_uuid: %s, table_id: %s, view_id: %s request columns error: %s', dtable_uuid, table_id, view_id, e)
        return []
    return columns


def get_related_users_dict(dtable_uuid, dtable_server_access_token):
    url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/{dtable_uuid}/related-users/'.format(dtable_uuid=dtable_uuid)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    related_users = []
    try:
        response = requests.get(url, headers=headers)
        related_users = response.json()['user_list']
    except Exception as e:
        logger.error('dtable_uuid: %s get related users error: %s', dtable_uuid, e)
    return {u['email']: u for u in related_users}


def _fill_msg_blanks(msg, column_blanks, col_name_dict, row, related_users_dict=None):
    for blank in column_blanks:
        if col_name_dict[blank]['type'] in [
            ColumnTypes.TEXT,
            ColumnTypes.DATE,
            ColumnTypes.LONG_TEXT,
            ColumnTypes.SINGLE_SELECT,
            ColumnTypes.URL,
            ColumnTypes.DURATION,
            ColumnTypes.NUMBER,
            ColumnTypes.EMAIL,
            ColumnTypes.FORMULA,
            ColumnTypes.LINK_FORMULA,
            ColumnTypes.AUTO_NUMBER,
            ColumnTypes.CTIME,
            ColumnTypes.MTIME
        ]:
            value = row.get(blank, '')
            msg = msg.replace('{' + blank + '}', str(value) if value else '')  # maybe value is None and str(None) is 'None'

        elif col_name_dict[blank]['type'] in [
            ColumnTypes.IMAGE,
            ColumnTypes.MULTIPLE_SELECT,
            ColumnTypes.LINK,
        ]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join(value) + ']') if value else '[]')  # maybe value is None

        elif col_name_dict[blank]['type'] in [ColumnTypes.FILE]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join([f['name'] for f in value]) + ']') if value else '[]')

        elif col_name_dict[blank]['type'] in [ColumnTypes.COLLABORATOR]:
            users = row.get(blank, [])
            if users is None:
                users = []
            if not related_users_dict:
                msg = msg.replace('{' + blank + '}', '[' + ', '.join(users) + ']')
            else:
                names = []
                for u in users:
                    user = related_users_dict.get(u)
                    name = user['name'] if user else value
                    names.append(name)
                msg = msg.replace('{' + blank + '}', '[' + ', '.join(names) + ']')

        elif col_name_dict[blank]['type'] in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            value = row.get(blank, '')
            if value is None:
                value = ''
            if not related_users_dict:
                msg = msg.replace('{' + blank + '}', value)
            else:
                user = related_users_dict.get(value)
                name = user['name'] if user else value
                msg = msg.replace('{' + blank + '}', name)

    return msg


def _get_column_blanks_and_related_users(dtable_uuid, dtable_server_access_token, blanks, columns):
    col_name_dict = {col['name']: col for col in columns}

    column_blanks = []
    need_related_users, related_users_dict = False, {}
    for blank in blanks:
        if blank in col_name_dict:
            column_blanks.append(blank)
            if col_name_dict[blank]['type'] in [
                ColumnTypes.COLLABORATOR,
                ColumnTypes.CREATOR,
                ColumnTypes.LAST_MODIFIER
            ]:
                need_related_users = True
    if not column_blanks:
        return [], col_name_dict, {}
    if need_related_users:
        related_users_dict = get_related_users_dict(dtable_uuid, dtable_server_access_token)

    return column_blanks, col_name_dict, related_users_dict


def gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token):
    if not msg:
        return msg

    # checkout all blanks to fill in
    # if no blanks, just return msg
    blanks = set(re.findall(r'\{([^{]*?)\}', msg))
    if not blanks:
        return msg

    columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)

    column_blanks, col_name_dict, related_users_dict = _get_column_blanks_and_related_users(dtable_uuid, dtable_server_access_token, blanks, columns)

    if not column_blanks:
        return msg

    # get row of table-view-row
    row_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/{dtable_uuid}/rows/{row_id}/'.format(dtable_uuid=dtable_uuid, row_id=row_id)
    headers = {'Authorization': 'Token ' + dtable_server_access_token.decode('utf-8')}
    params = {'table_id': table_id, 'convert_link_id': True}
    try:
        response = requests.get(row_url, params=params, headers=headers)
        row = response.json()
    except Exception as e:
        logger.error('dtable_uuid: %s, table_id: %s, row_id: %s, request row error: %s', dtable_uuid, table_id, row_id, e)
        return msg

    msg = _fill_msg_blanks(msg, column_blanks, col_name_dict, row, related_users_dict)

    return msg


def gen_notification_msg_with_row(msg, row, column_blanks, col_name_dict, related_users_dict):
    if not msg:
        return msg
    if not column_blanks:
        return msg

    return _fill_msg_blanks(msg, column_blanks, col_name_dict, row, related_users_dict)


def check_notification_rule(rule, message_table_id, row_id, column_keys, dtable_server_access_token, db_session=None):

    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    creator = rule[3]
    last_trigger_time = rule[4]
    dtable_uuid = rule[5]

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    users_column_key = action.get('users_column_key')
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')
    table_id = trigger['table_id']
    view_id = trigger['view_id']

    if message_table_id != table_id:
        return

    if not is_row_in_view(row_id, view_id, dtable_uuid, message_table_id, dtable_server_access_token):
        return
    user_msg_list = []

    if trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not is_trigger_time_satisfy(last_trigger_time):
            return

        target_column_keys = trigger.get('column_keys', [])
        watch_all_columns = trigger.get('watch_all_columns')
        if watch_all_columns is not None and not isinstance(watch_all_columns, bool):
            watch_all_columns = False

        # For compatibility with old code, there is no need to judge whether updated column_keys in target_column_keys when watch_all_columns is None
        # Only watch_all_columns is not None, need to judge whether send notification or not
        if watch_all_columns is not None and not watch_all_columns:
            has_msg_key_in_target_keys = False
            for msg_key in column_keys:
                if msg_key in target_column_keys:
                    has_msg_key_in_target_keys = True
                    break
            if not has_msg_key_in_target_keys:
                return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token),
            'row_id_list': [row_id],
        }

        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            users = list(set(users + users_from_cell))

        for user in users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    elif trigger['condition'] == CONDITION_FILTERS_SATISFY:
        filters = trigger.get('filters', [])
        if not filters:
            return

        target_column_keys = trigger.get('column_keys', [])
        watch_all_columns = trigger.get('watch_all_columns', False)

        if not watch_all_columns:
            has_msg_key_in_target_keys = False
            for msg_key in column_keys:
                if msg_key in target_column_keys:
                    has_msg_key_in_target_keys = True
                    break
            if not has_msg_key_in_target_keys:
                return

        filter_conjuntion = trigger.get('filter_conjunction', 'And')
        if not is_row_satisfy_filters(row_id, filters, filter_conjuntion, dtable_uuid, table_id, dtable_server_access_token):
            return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_FILTERS_SATISFY,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row_id(dtable_uuid, table_id, view_id, row_id, msg, dtable_server_access_token),
            'row_id_list': [row_id],
        }
        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            users = list(set(users + users_from_cell))

        for user in users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    else:
        return

    update_rule_last_trigger_time(rule_id, db_session)


def check_near_deadline_notification_rule(rule, db_session, timezone):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    dtable_uuid = rule[5]

    trigger = json.loads(trigger)
    action = json.loads(action)
    users = action.get('users', [])
    users_column_key = action.get('users_column_key')
    msg = action.get('default_msg', '')
    rule_name = trigger.get('rule_name', '')
    table_id = trigger['table_id']
    view_id = trigger['view_id']

    if trigger['condition'] != CONDITION_NEAR_DEADLINE:
        return

    date_column_name = trigger['date_column_name']
    alarm_days = trigger['alarm_days']
    table_id = trigger['table_id']
    view_id = trigger['view_id']
    notify_hour = trigger.get('notify_hour')

    try:
        cur_hour = int((datetime.utcnow() + pytz.timezone(timezone)._utcoffset).strftime('%H'))
    except Exception as e:
        logger.error('timezone: %s parse error: %s', timezone, e)
        cur_hour = int(time.strftime('%H'))

    if notify_hour != None:
        if int(notify_hour) != cur_hour:
            return
    else:
        if cur_hour != 12:
            return

    dtable_server_access_token = get_dtable_server_token(dtable_uuid)
    try:
        rows_near_deadline = list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token)
    except Exception as e:
        logger.error('list rows_near_deadline failed. error: {}'.format(e))
        return

    if not rows_near_deadline:
        return

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    related_users_dict = {}
    if blanks:
        columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)
        column_blanks, col_name_dict, related_users_dict = _get_column_blanks_and_related_users(dtable_uuid, dtable_server_access_token, blanks, columns)

    for row in rows_near_deadline[:25]:
        row_id = row['_id']

        if users_column_key:
            users_from_cell = list_users_by_column_key(dtable_uuid, table_id, view_id, row_id, users_column_key, dtable_server_access_token)
            to_users = list(set(users + users_from_cell))
        else:
            to_users = users

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_NEAR_DEADLINE,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row(msg, row, column_blanks, col_name_dict, related_users_dict),
            'row_id_list': [row_id],
        }

        user_msg_list = []
        for user in to_users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    update_rule_last_trigger_time(rule_id, db_session)
