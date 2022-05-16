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

from dtable_events.utils import is_valid_email, uuid_str_to_36_chars
from dtable_events.utils.constants import ColumnTypes
from dtable_events.cache import redis_cache as cache

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
    import seahub.settings as seahub_settings
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)


CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_ROWS_ADDED = 'rows_added'
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


def scan_triggered_notification_rules(event_data, db_session):
    row = event_data.get('row')
    converted_row = event_data.get('converted_row')
    message_dtable_uuid = event_data.get('dtable_uuid', '')
    table_id = event_data.get('table_id', '')
    rule_id = event_data.get('notification_rule_id')
    op_type = event_data.get('op_type')
    if not row or not converted_row or not message_dtable_uuid or not table_id or not rule_id:
        logger.error(f'redis event data not valid, event_data = {event_data}')
        return

    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules WHERE run_condition='per_update'" \
          "AND dtable_uuid=:dtable_uuid AND is_valid=1 AND id=:rule_id"
    rules = db_session.execute(sql, {'dtable_uuid': message_dtable_uuid, 'rule_id': rule_id})

    dtable_server_access_token = get_dtable_server_token(message_dtable_uuid)
    for rule in rules:
        try:
            trigger_notification_rule(rule, table_id, row, converted_row, dtable_server_access_token, db_session, op_type)
        except Exception as e:
            logger.error(f'check rule failed. {rule}, error: {e}')
    db_session.commit()


def list_users_by_column_name(dtable_uuid, table_id, view_id, row_id, column_name, dtable_server_access_token):
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/rows/' + row_id + '/?from=dtable_events'
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
    params = {
        'table_id': table_id,
        'view_id': view_id,
        # 'convert': True,
    }
    res = requests.get(url, headers=headers, params=params)

    if res.status_code != 200:
        logger.error(f'dtable {dtable_uuid} failed to list_users_by_column_name {res.text}')

    rowdict = json.loads(res.content)
    user_list = rowdict.get(column_name, [])
    if not user_list:
        return []
    if isinstance(user_list, str):
        return [user_list]
    return user_list


def send_notification(dtable_uuid, user_msg_list, dtable_server_access_token):
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/notifications-batch/?from=dtable_events'
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
    body = {
        'user_messages': user_msg_list,
    }
    res = requests.post(url, headers=headers, json=body)

    if res.status_code != 200:
        logger.error(f'dtable {dtable_uuid} failed to send_notification {res.text}')


def deal_invalid_rule(rule_id, db_session):
    sql = "UPDATE dtable_notification_rules SET is_valid=:is_valid WHERE id=:rule_id"
    try:
        db_session.execute(sql, {'is_valid': 0, 'rule_id': rule_id})
    except Exception as e:
        logger.error(e)


def list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token, rule_id=None, db_session=None):
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/internal/dtables/' + uuid_str_to_36_chars(dtable_uuid) + '/list-rows-near-deadline/?from=dtable_events'
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
    now_date = date.today()
    now_plus_alarm_date = now_date + timedelta(days=int(alarm_days))
    query_params = {
        'table_id': table_id,
        'view_id': view_id,
        'date_column_name': date_column_name,
        'now_date': str(now_date),
        'end_date': str(now_plus_alarm_date),
        'limit': 25
    }
    try:
        res = requests.get(url, headers=headers, params=query_params)
    except Exception as e:
        logger.error('dtable: %s request list-rows-near-deadline error: %s', dtable_uuid, e)
        return []

    if res.status_code == 404:
        deal_invalid_rule(rule_id, db_session)
    if res.status_code != 200:
        logger.error('dtable: %s request list-rows-near-deadline error status code: %s, response text: %s', dtable_uuid, res.status_code, res.text)
        return []

    rows = json.loads(res.content).get('rows', [])
    return rows


def get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token):
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/columns/?from=dtable_events'
    headers = {'Authorization': 'Token ' + dtable_server_access_token}
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


def _get_dtable_metadata(dtable_uuid):
    api_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    access_token = get_dtable_server_token(dtable_uuid)
    metadata_url = api_url.rstrip('/') + '/api/v1/dtables/' + dtable_uuid + '/metadata/?from=dtable_events'
    headers = {'Authorization': 'Token ' + access_token}
    response = requests.get(metadata_url, headers=headers)
    return response.json().get('metadata')


def _get_geolocation_infos(cell_value_dict):
    if not isinstance(cell_value_dict, dict):
        return ''
    info_list = []
    province = cell_value_dict.get('province', '')
    city = cell_value_dict.get('city', '')
    district = cell_value_dict.get('district', '')
    detail = cell_value_dict.get('detail', '')
    country_region = cell_value_dict.get('country_region', '')

    lng = cell_value_dict.get('lng', '')
    lat = cell_value_dict.get('lat', '')

    if country_region:
        info_list.append(country_region)
    if province:
        info_list.append(province)
    if city:
        info_list.append(city)
    if district:
        info_list.append(district)
    if detail:
        info_list.append(detail)

    if lng:
        info_list.append("lng: %s" % lng)
    if lat:
        info_list.append("lat: %s" % lat)

    return info_list and " ".join(info_list) or ''

def _fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata=None):
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
            ColumnTypes.AUTO_NUMBER,
            ColumnTypes.CTIME,
            ColumnTypes.MTIME
        ]:
            value = row.get(blank, '')
            msg = msg.replace('{' + blank + '}', str(value) if value else '')  # maybe value is None and str(None) is 'None'

        elif col_name_dict[blank]['type'] in [
            ColumnTypes.IMAGE,
            ColumnTypes.MULTIPLE_SELECT
        ]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join(value) + ']') if value else '[]')  # maybe value is None
        elif col_name_dict[blank]['type'] in [ColumnTypes.LINK]:
            value = row.get(blank, [])

            msg = msg.replace('{' + blank + '}', ('[' + ', '.join([f['display_value'] for f in value]) + ']') if value else '[]')

        elif col_name_dict[blank]['type'] in [ColumnTypes.FILE]:
            value = row.get(blank, [])
            msg = msg.replace('{' + blank + '}', ('[' + ', '.join([f['name'] for f in value]) + ']') if value else '[]')

        elif col_name_dict[blank]['type'] in [ColumnTypes.COLLABORATOR]:
            users = row.get(blank, [])
            if users is None:
                names = []
            else:
                names_dict = get_nickname_by_usernames(users, db_session)
                names = [names_dict.get(user) for user in users if user in names_dict]
            msg = msg.replace('{' + blank + '}', '[' + ', '.join(names) + ']')

        elif col_name_dict[blank]['type'] in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            value = row.get(blank, '')
            if value is None:
                name = ''
            else:
                name = get_nickname_by_usernames([value], db_session).get(value, '')
            msg = msg.replace('{' + blank + '}', name)

        elif col_name_dict[blank]['type'] in [ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA]:
            # Fill formula blanks
            # If result_type of formula is 'column', which indicates that real result_type could be like collaborator
            # we need to fill blanks with nicknames,
            # else just transfer value to str to fill blanks.
            # Judge whether value is like collaborator or not is base on metadata of dtable
            value = row.get(blank)

            # If result of formula is a string or None, just replace directly
            if isinstance(value, str) or value is None:
                msg = msg.replace('{' + blank + '}', value if value else '')
                continue

            formula_data = col_name_dict[blank].get('data')
            if not formula_data:
                continue
            # If not result_type is not 'column', just return str(value)
            if formula_data.get('result_type') != 'column':
                msg = msg.replace('{' + blank + '}', str(value) if value else '')
                continue

            # According `display_column_key`, `linked_table_id` and metadata of dtable,
            # judge whether result_type of formula is like collaborator or not.
            display_column_key, linked_table_id = formula_data.get('display_column_key'), formula_data.get('linked_table_id')
            target_column_type = None
            if not dtable_metadata:
                try:
                    dtable_metadata = _get_dtable_metadata(dtable_uuid)
                except Exception as e:
                    logger.error('request dtable metadata in fill msg error: %s', e)
                    continue
            for table in dtable_metadata.get('tables', []):
                if table.get('_id') == linked_table_id:
                    columns = table.get('columns', [])
                    for col in columns:
                        if col.get('key') == display_column_key:
                            target_column_type = col.get('type')
                            break
                if target_column_type:
                    break

            # If result_type is like collaborator, fill blanks with nicknames
            if target_column_type in [ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
                if value is None:
                    msg = msg.replace('{' + blank + '}', '[]')
                    continue
                names_dict = get_nickname_by_usernames(value, db_session)
                names = [names_dict.get(user) for user in value if user in names_dict]
                msg = msg.replace('{' + blank + '}', '[' + ', '.join(names) + ']')

            # else just fill str(value)
            else:
                if value is None:
                    msg = msg.replace('{' + blank + '}', '')
                elif isinstance(value, list):
                    msg = msg.replace('{' + blank + '}', '[' + ', '.join([str(v) for v in value]) + ']')
                else:
                    msg = msg.replace('{' + blank + '}', str(value))

        elif col_name_dict[blank]['type'] in [ColumnTypes.GEOLOCATION]:
            value = row.get(blank, '')
            value = value and _get_geolocation_infos(value) or ''
            msg = msg.replace('{' + blank + '}', str(value) if value else '')


    return msg


def _get_column_blanks(blanks, columns):
    col_name_dict = {col['name']: col for col in columns}

    column_blanks = []
    for blank in blanks:
        if blank in col_name_dict:
            column_blanks.append(blank)
    if not column_blanks:
        return [], col_name_dict

    return column_blanks, col_name_dict


def gen_notification_msg_with_row(dtable_uuid, msg, row, column_blanks, col_name_dict, db_session, dtable_metadata=None):
    if not msg:
        return msg
    if not column_blanks:
        return msg

    return _fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata=dtable_metadata)


def _get_column_by_key(dtable_metadata, table_id, column_key):
    table = None
    for t in dtable_metadata.get('tables', []):
        if t.get('_id') == table_id:
            table = t

    if not table:
        return None

    for col in table.get('columns'):
        if col.get('key') == column_key:
            return col

    return None


def trigger_notification_rule(rule, message_table_id, row, converted_row, dtable_server_access_token, db_session, op_type):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
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

    user_msg_list = []

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    if blanks:
        columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)
        column_blanks, col_name_dict = _get_column_blanks(blanks, columns)

    if op_type == 'modify_row' and trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not is_trigger_time_satisfy(last_trigger_time):
            return

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row(dtable_uuid, msg, converted_row, column_blanks, col_name_dict, db_session),
            'row_id_list': [row['_id']],
        }

        if users_column_key:
            dtable_metadata = _get_dtable_metadata(dtable_uuid)
            user_column = _get_column_by_key(dtable_metadata, table_id, users_column_key)
            if user_column:
                users_column_name = user_column.get('name')
                users_from_column = converted_row.get(users_column_name, [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(users + users_from_column))
            else:
                logger.warning('notification rule: %s notify user column: %s invalid', rule_id, users_column_key)

        for user in users:
            if not is_valid_email(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    elif (op_type == 'modify_row' and trigger['condition'] == CONDITION_FILTERS_SATISFY) or \
         (op_type in ('insert_row', 'append_rows') and trigger['condition'] == CONDITION_ROWS_ADDED):
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_FILTERS_SATISFY,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row(dtable_uuid, msg, converted_row, column_blanks, col_name_dict, db_session),
            'row_id_list': [row['_id']],
        }
        if users_column_key:
            dtable_metadata = _get_dtable_metadata(dtable_uuid)
            user_column = _get_column_by_key(dtable_metadata, table_id, users_column_key)
            users_column_name = user_column.get('name')
            users_from_column = converted_row.get(users_column_name, [])
            if not users_from_column:
                users_from_column = []
            if not isinstance(users_from_column, list):
                users_from_column = [users_from_column, ]
            users = list(set(users + users_from_column))

        for user in users:
            if not is_valid_email(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    else:
        return

    update_rule_last_trigger_time(rule_id, db_session)


def trigger_near_deadline_notification_rule(rule, db_session):
    rule_id = rule[0]
    trigger = rule[1]
    action = rule[2]
    dtable_uuid = rule[4]

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
    cur_datetime = datetime.now()

    cur_hour = int(cur_datetime.hour)


    if notify_hour != None:
        if int(notify_hour) != cur_hour:

            return
    else:
        if cur_hour != 12:
            return

    dtable_server_access_token = get_dtable_server_token(dtable_uuid)
    try:
        rows_near_deadline = list_rows_near_deadline(dtable_uuid, table_id, view_id, date_column_name, alarm_days, dtable_server_access_token, rule_id, db_session)
    except Exception as e:
        logger.error('dtable: %s list rows_near_deadline failed. error: %s', dtable_uuid, e)
        return

    if not rows_near_deadline:
        return

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    if blanks:
        columns = get_table_view_columns(dtable_uuid, table_id, view_id, dtable_server_access_token)
        column_blanks, col_name_dict = _get_column_blanks(blanks, columns)

    dtable_metadata = _get_dtable_metadata(dtable_uuid)
    for row in rows_near_deadline[:25]:
        row_id = row['_id']

        if users_column_key:
            user_column = _get_column_by_key(dtable_metadata, table_id, users_column_key)
            users_column_name = user_column.get('name')
            users_from_cell = list_users_by_column_name(dtable_uuid, table_id, view_id, row_id, users_column_name, dtable_server_access_token)
            to_users = list(set(users + users_from_cell))
        else:
            to_users = users

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_NEAR_DEADLINE,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_notification_msg_with_row(dtable_uuid, msg, row, column_blanks, col_name_dict, db_session, dtable_metadata=dtable_metadata),
            'row_id_list': [row_id],
        }

        user_msg_list = []
        for user in to_users:
            if not is_valid_email(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        send_notification(dtable_uuid, user_msg_list, dtable_server_access_token)

    update_rule_last_trigger_time(rule_id, db_session)
