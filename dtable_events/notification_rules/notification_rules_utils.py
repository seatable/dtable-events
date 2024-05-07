# -*- coding: utf-8 -*-
import json
import logging
import re
import time
from datetime import datetime

from sqlalchemy import text
import jwt
import requests

from dtable_events import filter2sql
from dtable_events.app.config import DTABLE_PRIVATE_KEY, DTABLE_WEB_SERVICE_URL, INNER_DTABLE_DB_URL
from dtable_events.app.metadata_cache_managers import RuleIntentMetadataCacheManger, RuleIntervalMetadataCacheManager
from dtable_events.notification_rules.utils import get_nickname_by_usernames
from dtable_events.utils import is_valid_email, uuid_str_to_36_chars, get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes, FormulaResultType
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.notification_rules.message_formatters import create_formatter_params, formatter_map

logger = logging.getLogger(__name__)


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
    db_session.execute(text(cmd), {'new_time': datetime.utcnow(), 'rule_id': rule_id})


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
    message_dtable_uuid = event_data.get('dtable_uuid', '')
    table_id = event_data.get('table_id', '')
    rule_id = event_data.get('notification_rule_id')
    op_type = event_data.get('op_type')
    if not row or not message_dtable_uuid or not table_id or not rule_id:
        logger.error(f'redis event data not valid, event_data = {event_data}')
        return

    sql = "SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules WHERE run_condition='per_update'" \
          "AND dtable_uuid=:dtable_uuid AND is_valid=1 AND id=:rule_id"
    rules = db_session.execute(text(sql), {'dtable_uuid': message_dtable_uuid, 'rule_id': rule_id})

    rule_intent_metadata_cache_manager = RuleIntentMetadataCacheManger()
    for rule in rules:
        try:
            trigger_notification_rule(rule, table_id, row, db_session, op_type, rule_intent_metadata_cache_manager)
        except Exception as e:
            logger.exception(e)
            logger.error(f'check rule failed. {rule}, error: {e}')
    db_session.commit()


def send_notification(dtable_uuid, user_msg_list, dtable_server_access_token):
    api_url = get_inner_dtable_server_url()
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
        db_session.execute(text(sql), {'is_valid': 0, 'rule_id': rule_id})
    except Exception as e:
        logger.error(e)


def list_rows_near_deadline_with_dtable_db(dtable_metadata, table_id, view_id, date_column_name, alarm_days, dtable_db_api):
    """
    return: rows -> list or None, metate -> dict or None, is_valid -> True/False
    """
    table, view, date_column = None, None, None
    for tmp_table in dtable_metadata['tables']:
        if tmp_table['_id'] == table_id:
            table = tmp_table
            break
    if not table:
        return [], None, False
    for tmp_view in table.get('views', []):
        if tmp_view['_id'] == view_id:
            view = tmp_view
            break
    if not view:
        return [], None, False
    for tmp_column in table['columns']:
        if tmp_column['name'] == date_column_name:
            date_column = tmp_column
            break
    if not date_column:
        return [], None, False
    if date_column['type'] != ColumnTypes.DATE:
        if date_column['type'] not in [ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA]:
            return [], None, False
        column_data = date_column.get('data') or {}
        if column_data.get('result_type') != FormulaResultType.DATE:
            return [], None, False
    filters = view.get('filters', [])
    filter_conjunction = view.get('filter_conjunction', 'And')
    filter_conditions = {
        'start': 0,
        'limit': 25
    }
    new_filters = []
    for item in filters:
        if item.get('filter_predicate') in ('include_me', 'is_current_user_ID'):
            return [], None, False
        new_filters.append(item)
    filter_conditions['filter_groups'] = [{
        'filters': new_filters,
        'filter_conjunction': filter_conjunction
    }]
    filter_conditions['filter_groups'].append({
        'filters': [{
                "column_name": date_column_name,
                "filter_predicate": "is_before",
                "filter_term": alarm_days + 1,
                "filter_term_modifier": "number_of_days_from_now"
            }, {
                "column_name": date_column_name,
                "filter_predicate": "is_on_or_after",
                "filter_term": "",
                "filter_term_modifier": "today"
            }
        ],
        'filter_conjunction': 'And'
    })
    filter_conditions['group_conjunction'] = 'And'
    try:
        sql = filter2sql(table['name'], table['columns'], filter_conditions, by_group=True)
        logger.debug('sql: %s', sql)
        rows, metadata = dtable_db_api.query(sql, convert=False)
    except Exception as e:
        logger.warning('list rows near deadline error: %s' % e)
        return [], None, False
    return rows, metadata, True


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

def convert_zero_in_value(value):

    if value == 0:
        return '0'

    return value


def fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session):
    for blank in column_blanks:
        value = row.get(col_name_dict[blank]['key'])
        column_type = col_name_dict[blank]['type']
        formatter_class = formatter_map.get(column_type)
        if not formatter_class:
            continue
        params = create_formatter_params(column_type, value=value, db_session=db_session)
        if value is None:
            message = formatter_class(col_name_dict[blank]).format_empty_message()
            msg = msg.replace('{' + blank + '}', str(message))
            continue
        try:
            message = formatter_class(col_name_dict[blank]).format_message(**params)
            msg = msg.replace('{' + blank + '}', str(message))
        except Exception as e:
            logger.exception(e)
            msg = msg.replace('{' + blank + '}', '')

    return msg


def get_column_blanks(blanks, columns):
    col_name_dict = {col['name']: col for col in columns}

    column_blanks = []
    for blank in blanks:
        if blank in col_name_dict:
            column_blanks.append(blank)
    if not column_blanks:
        return [], col_name_dict

    return column_blanks, col_name_dict


def gen_noti_msg_with_sql_row(msg, row, column_blanks, col_name_dict, db_session):
    if not msg:
        return msg
    if not column_blanks:
        return msg
    return fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session)


def get_column_by_key(dtable_metadata, table_id, column_key):
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


def trigger_notification_rule(rule, message_table_id, row, db_session, op_type, rule_intent_metadata_cache_manager: RuleIntentMetadataCacheManger):
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

    dtable_server_api = DTableServerAPI('notification-rule', dtable_uuid, get_inner_dtable_server_url(), access_token_timeout=3600)
    dtable_db_api = DTableDBAPI('notification-rule', dtable_uuid, INNER_DTABLE_DB_URL)
    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
    dtable_metadata = rule_intent_metadata_cache_manager.get_metadata(dtable_uuid)
    target_table, target_view = None, None
    for table in dtable_metadata['tables']:
        if table['_id'] == table_id:
            target_table = table
            break
    if not target_table:
        deal_invalid_rule(rule_id, db_session)
        return
    for view in target_table['views']:
        if view['_id'] == view_id:
            target_view = view
            break
    if not target_view:
        deal_invalid_rule(rule_id, db_session)
        return

    related_users_dict = {user['email']: user for user in dtable_web_api.get_related_users(dtable_uuid)}
    temp_users = []
    for user in users:
        if user and user not in related_users_dict:
            logger.warning('notify rule: %s has invalid user: %s', rule_id, user)
            deal_invalid_rule(rule_id, db_session)
            return
        if user:
            temp_users.append(user)
    users = temp_users
    user_msg_list = []

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    if blanks:
        columns = target_table['columns']
        column_blanks, col_name_dict = get_column_blanks(blanks, columns)

    if op_type in ('modify_row', 'modify_rows', 'add_link', 'update_links', 'update_rows_links', 'remove_link') and trigger['condition'] == CONDITION_ROWS_MODIFIED:
        if not is_trigger_time_satisfy(last_trigger_time):
            return

        row_id = row['_id']
        sql = f"SELECT * FROM `{target_table['name']}` WHERE _id='{row_id}'"
        rows, _ = dtable_db_api.query(sql, convert=False)
        if not rows:
            return
        sql_row = rows[0]

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_noti_msg_with_sql_row(msg, sql_row, column_blanks, col_name_dict, db_session),
            'row_id_list': [row['_id']],
        }

        if users_column_key:
            user_column = get_column_by_key(dtable_metadata, table_id, users_column_key)
            if user_column:
                users_from_column = sql_row.get(user_column['key'], [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(users + [user for user in users_from_column if user in related_users_dict]))
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
        send_notification(dtable_uuid, user_msg_list, dtable_server_api.access_token)

    elif (op_type in ('modify_row', 'modify_rows', 'add_link', 'update_links', 'update_rows_links') and trigger['condition'] == CONDITION_FILTERS_SATISFY) or \
         (op_type in ('insert_row', 'append_rows', 'insert_rows') and trigger['condition'] == CONDITION_ROWS_ADDED):

        row_id = row['_id']
        sql = f"SELECT * FROM `{target_table['name']}` WHERE _id='{row_id}'"
        rows, _ = dtable_db_api.query(sql, convert=False)
        if not rows:
            return
        sql_row = rows[0]

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_FILTERS_SATISFY,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_noti_msg_with_sql_row(msg, sql_row, column_blanks, col_name_dict, db_session),
            'row_id_list': [row['_id']],
        }
        if users_column_key:
            user_column = get_column_by_key(dtable_metadata, table_id, users_column_key)
            if user_column:
                users_from_column = sql_row.get(user_column['key'], [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(users + [user for user in users_from_column if user in related_users_dict]))
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
        send_notification(dtable_uuid, user_msg_list, dtable_server_api.access_token)

    else:
        return

    update_rule_last_trigger_time(rule_id, db_session)


def trigger_near_deadline_notification_rule(rule, db_session, rule_interval_metadata_cache_manager: RuleIntervalMetadataCacheManager):
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

    dtable_server_api = DTableServerAPI('notification-rule', dtable_uuid, get_inner_dtable_server_url(), access_token_timeout=3600)
    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
    dtable_db_api = DTableDBAPI('dtable-events', dtable_uuid, INNER_DTABLE_DB_URL)
    dtable_metadata = rule_interval_metadata_cache_manager.get_metadata(dtable_uuid)
    target_table, target_view = None, None
    for table in dtable_metadata['tables']:
        if table['_id'] == table_id:
            target_table = table
            break
    if not target_table:
        deal_invalid_rule(rule_id, db_session)
        return
    for view in target_table['views']:
        if view['_id'] == view_id:
            target_view = view
            break
    if not target_view:
        deal_invalid_rule(rule_id, db_session)
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

    related_users_dict = {user['email']: user for user in dtable_web_api.get_related_users(dtable_uuid)}
    temp_users = []
    for user in users:
        if user and user not in related_users_dict:
            logger.warning('notify rule: %s has invalid user: %s', rule_id, user)
            deal_invalid_rule(rule_id, db_session)
            return
        if user:
            temp_users.append(user)
    users = temp_users

    try:
        rows_near_deadline, sql_metadata, is_valid = list_rows_near_deadline_with_dtable_db(dtable_metadata, table_id, view_id, date_column_name, alarm_days, dtable_db_api)
    except Exception as e:
        logger.exception(e)
        logger.error('dtable: %s list rows_near_deadline failed. error: %s', dtable_uuid, e)
        return

    if is_valid is False:
        deal_invalid_rule(rule_id, db_session)

    if not rows_near_deadline:
        return

    blanks, column_blanks, col_name_dict = set(re.findall(r'\{([^{]*?)\}', msg)), None, None
    if blanks:
        column_blanks, col_name_dict = get_column_blanks(blanks, sql_metadata)

    to_users = []
    for row in rows_near_deadline[:25]:
        row_id = row['_id']

        if users_column_key:
            user_column = get_column_by_key(dtable_metadata, table_id, users_column_key)
            if user_column:
                temp_users_from_cell = row.get(users_column_key, [])
                if isinstance(temp_users_from_cell, list):
                    users_from_cell = temp_users_from_cell
                elif isinstance(temp_users_from_cell, str):
                    users_from_cell = [temp_users_from_cell]
                else:
                    users_from_cell = []
                to_users = list(set(users + [user for user in users_from_cell if user in related_users_dict]))
            else:
                logger.warning('notification rule: %s notify user column: %s invalid', rule_id, users_column_key)
        else:
            to_users = users

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_NEAR_DEADLINE,
            'rule_id': rule.id,
            'rule_name': rule_name,
            'msg': gen_noti_msg_with_sql_row(msg, row, column_blanks, col_name_dict, db_session),
            'row_id_list': [row_id],
        }

        user_msg_list = []
        for user in to_users:
            if isinstance(user, str) and not is_valid_email(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        send_notification(dtable_uuid, user_msg_list, dtable_server_api.access_token)

    update_rule_last_trigger_time(rule_id, db_session)
