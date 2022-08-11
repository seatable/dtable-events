import json
import logging
import re
import time
import os
from uuid import UUID
from datetime import datetime, timedelta

import jwt
import requests

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, SEATABLE_FAAS_AUTH_TOKEN, \
    SEATABLE_FAAS_URL
from dtable_events.automations.models import BoundThirdPartyAccounts
from dtable_events.dtable_io import send_wechat_msg, send_email_msg, send_dingtalk_msg
from dtable_events.notification_rules.notification_rules_utils import _fill_msg_blanks as fill_msg_blanks, \
    send_notification
from dtable_events.utils import is_valid_email, get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import DTableServerAPI


logger = logging.getLogger(__name__)

PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'
PER_MONTH = 'per_month'

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_ROWS_ADDED = 'rows_added'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'
CONDITION_PERIODICALLY = 'run_periodically'
CONDITION_PERIODICALLY_BY_CONDITION = 'run_periodically_by_condition'

MESSAGE_TYPE_AUTOMATION_RULE = 'automation_rule'

AUTO_RULE_TRIGGER_LIMIT_PER_MINUTE = 10
AUTO_RULE_TRIGGER_TIMES_PER_MINUTE_TIMEOUT = 60



def get_third_party_account(session, account_id):
    account_query = session.query(BoundThirdPartyAccounts).filter(
        BoundThirdPartyAccounts.id == account_id
    )
    account = account_query.first()
    if account:
        return account.to_dict()
    else:
        logger.warning("Third party account %s does not exists." % account_id)
        return None

def email2list(email_str, split_pattern='[,，]'):
    email_list = [value.strip() for value in re.split(split_pattern, email_str) if value.strip()]
    return email_list


def format_time_by_offset(offset, format_length):
    cur_datetime = datetime.now()
    cur_datetime_offset = cur_datetime + timedelta(days=offset)
    if format_length == 2:
        return cur_datetime_offset.strftime('%Y-%m-%d %H:%M')
    elif format_length == 1:
        return cur_datetime_offset.strftime('%Y-%m-%d')


def parse_column_value(column, value):
    if column.get('type') == ColumnTypes.SINGLE_SELECT:
        select_options = column.get('data', {}).get('options', [])
        for option in select_options:
            if value == option.get('id'):
                return option.get('name')
    elif column.get('type') == ColumnTypes.MULTIPLE_SELECT:
        m_select_options = column.get('data', {}).get('options', [])
        if isinstance(value, list):
            parse_value_list = []
            for option in m_select_options:
                if option.get('id') in value:
                    option_name = option.get('name')
                    parse_value_list.append(option_name)
            return parse_value_list
    else:
        return value


def is_valid_username(user):
    if not user:
        return False

    return is_valid_email(user)


class ContextInvalid(Exception):
    pass


class BaseContext:

    def __init__(self, dtable_uuid, table_id, db_session, view_id=None, caller='dtable-events'):
        self.dtable_uuid = str(UUID(dtable_uuid))
        self.table_id = table_id
        self.view_id = view_id
        self.db_session = db_session
        self.caller = caller

        self.dtable_server_api = DTableServerAPI(caller, self.dtable_uuid, get_inner_dtable_server_url())

        self._dtable_metadata = None
        self._table = None
        self._access_token = None
        self._headers = None
        self._view = None
        self._columns_dict = None
        self._related_users = None
        self._related_users_dict = None

        self._can_run_python = None
        self._scripts_running_limit = None

    @property
    def access_token(self):
        if self._access_token:
            return self._access_token
        payload = {
            'username': self.caller,
            'exp': int(time.time()) + 60 * 60 * 15,
            'dtable_uuid': str(UUID(self.dtable_uuid)),
            'permission': 'rw',
            'id_in_org': ''
        }
        access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, 'HS256')
        self._access_token = access_token
        return self._access_token

    @property
    def headers(self):
        if self._headers:
            return self._headers
        return {'Authorization': 'Token ' + self.access_token}

    @property
    def dtable_metadata(self):
        if self._dtable_metadata:
            return self._dtable_metadata
        self._dtable_metadata = self.dtable_server_api.get_metadata()
        if not self._dtable_metadata:
            raise ContextInvalid('get metadata error')
        return self._dtable_metadata

    @property
    def table(self):
        if self._table:
            return self._table
        for table in self.dtable_metadata['tables']:
            if table['_id'] == self.table_id:
                self._table = table
                break
        if not self._table:
            raise ContextInvalid('dtable: %s self.table: %s not found' % (self.dtable_uuid, self.table_id))
        return self._table

    @property
    def columns_dict(self):
        if self._columns_dict:
            return self._columns_dict
        self._columns_dict = {col['key']: col for col in self.table['columns']}
        return self._columns_dict

    @property
    def view(self):
        if self._view:
            return self._view
        if not self.view_id:
            return None
        for view in self.table['views']:
            if view['_id'] == self.view_id:
                self._view = view
                break
        if not self._view:
            raise ContextInvalid('dtable: %s self.table: %s self.view: %s not found' % (self.dtable_uuid, self.table_id, self.view_id))
        return self._view

    @property
    def related_users(self):
        if not self._related_users:
            url = '%(server_url)s/api/v2.1/dtables/%(dtable_uuid)s/related-users/' % {
                'server_url': DTABLE_WEB_SERVICE_URL.strip('/'),
                'dtable_uuid': self.dtable_uuid
            }
            self._related_users = ['']
        return self._related_users

    @property
    def related_users_dict(self):
        pass

    @property
    def can_run_python(self):
        return self._can_run_python

    @can_run_python.setter
    def can_run_python(self, can_run_python):
        self._can_run_python = can_run_python

    @property
    def scripts_running_limit(self):
        return self._scripts_running_limit

    @scripts_running_limit.setter
    def scripts_running_limit(self, limit):
        self._scripts_running_limit = limit

    def get_table_by_id(self, table_id):
        for table in self.dtable_metadata['tables']:
            if table['_id'] == table_id:
                return table
        return None

    def get_table_column_by_key(self, table_id, column_key):
        table = self.get_table_by_id(table_id)
        if not table:
            return None
        for col in table['columns']:
            if col['key'] == column_key:
                return col
        return None

    def get_temp_api_token(self, username=None, app_name=None):
        payload = {
            'dtable_uuid': self.dtable_uuid,
            'exp': int(time.time()) + 60 * 60,
        }
        if username:
            payload['username'] = username
        if app_name:
            payload['app_name'] = app_name
        temp_api_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
        return temp_api_token

    def get_converted_row(self, table_id, row_id):
        table = self.get_table_by_id(table_id)
        logger.debug('table_id: %s table_name: %s row_id: %s', table and table['name'], table_id, row_id)
        if not table:
            logger.error('dtable: %s table: %s not found', self.dtable_uuid, table_id)
            return None
        try:
            converted_row = self.dtable_server_api.get_row(table['name'], row_id)
            if not converted_row:
                logger.error('dtable: %s table: %s row: %s not found or parse error', self.dtable_uuid, table_id, row_id)
                return None
        except Exception as e:
            logger.error('dtable: %s table: %s row: %s error: %s', self.dtable_uuid, table_id, row_id, e)
            return None
        return converted_row


class ActionInvalid(Exception):
    pass


class BaseAction:

    RUN_SCRIPT_URL = SEATABLE_FAAS_URL.strip('/') + '/run-script/'

    VALID_COLUMN_TYPES = []

    def __init__(self, context: BaseContext):
        self.context = context
        self.table = self.context.table

    def generate_real_msg(self, msg, converted_row):
        if not converted_row:
            return msg
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        col_name_dict = {col.get('name'): col for col in self.context.table['columns']}
        column_blanks = [blank for blank in blanks if blank in col_name_dict]
        if not column_blanks:
            return msg
        return fill_msg_blanks(self.context.dtable_uuid, msg, column_blanks, col_name_dict, converted_row, self.context.db_session, self.context.dtable_metadata)

    def batch_generate_real_msgs(self, msg, converted_rows):
        return [self.generate_real_msg(msg, converted_row) for converted_row in converted_rows]

    def generate_filter_updates(self, add_or_updates, table):
        filter_updates = {}
        for col in table['columns']:
            if col['type'] not in self.VALID_COLUMN_TYPES:
                continue
            col_name = col['name']
            col_type = col['type']
            col_key = col['key']
            if col_key in add_or_updates:
                if col_type == ColumnTypes.DATE:
                    time_format = col.get('data', {}).get('format', '')
                    format_length = len(time_format.split(" "))
                    try:
                        time_dict = add_or_updates.get(col_key)
                        set_type = time_dict.get('set_type')
                        if set_type == 'specific_value':
                            time_value = time_dict.get('value')
                            filter_updates[col_name] = time_value
                        elif set_type == 'relative_date':
                            offset = time_dict.get('offset')
                            filter_updates[col_name] = format_time_by_offset(int(offset), format_length)
                    except:
                        logger.error(e)
                        filter_updates[col_name] = add_or_updates.get(col_key)
                else:
                    filter_updates[col_name] = parse_column_value(col, add_or_updates.get(col_key))
        return filter_updates

    def do_action(self):
        pass


class NotifyAction(BaseAction):

    NOTIFY_TYPE_NOTIFICATION_RULE = 'notify_type_notification_rule'
    NOTIFY_TYPE_AUTOMATION_RULE = 'notify_type_automation_rule'
    NOTIFY_TYPE_WORKFLOW = 'notify_type_workflow'

    NOTIFY_TYPES = [
        NOTIFY_TYPE_NOTIFICATION_RULE,
        NOTIFY_TYPE_AUTOMATION_RULE,
        NOTIFY_TYPE_WORKFLOW
    ]

    MSG_TYPES_DICT = {
        NOTIFY_TYPE_NOTIFICATION_RULE: 'notification_rules',
        NOTIFY_TYPE_AUTOMATION_RULE: 'automation_rules',
        NOTIFY_TYPE_WORKFLOW: 'workflows'
    }

    def __init__(self, context: BaseContext, users, msg, notify_type, converted_row=None,
                users_column_key=None, condition=None, rule_id=None, rule_name=None,
                workflow_token=None, workflow_name=None, workflow_task_id=None):
        super().__init__(context)
        self.users = users
        self.notify_type = notify_type
        self.users_column_key = users_column_key
        self.users_column = self.context.columns_dict.get(self.users_column_key)
        self.msg = msg
        self.converted_row = converted_row
        if notify_type == self.NOTIFY_TYPE_NOTIFICATION_RULE:
            if not condition:
                raise ActionInvalid('condition invalid')
            if not rule_id:
                raise ActionInvalid('rule_id invalid')
            if not rule_name:
                raise ActionInvalid('rule_name invalid')
            self.detail = {
                'table_id': context.table_id,
                'view_id': context.view_id,
                'condition': condition,
                'rule_id': rule_id,
                'rule_name': rule_name,
                'row_id_list': [converted_row['_id']] if converted_row else []
            }
        elif notify_type == self.NOTIFY_TYPE_AUTOMATION_RULE:
            if not condition:
                raise ActionInvalid('condition invalid')
            if not rule_id:
                raise ActionInvalid('rule_id invalid')
            if not rule_name:
                raise ActionInvalid('rule_name invalid')
            self.detail = {
                'table_id': context.table_id,
                'view_id': context.view_id,
                'condition': condition,
                'rule_id': rule_id,
                'rule_name': rule_name,
                'row_id_list': [converted_row['_id']] if converted_row else []
            }
        elif notify_type == self.NOTIFY_TYPE_WORKFLOW:
            if not workflow_token:
                raise ActionInvalid('workflow_token invalid')
            if not workflow_name:
                raise ActionInvalid('workflow_name invalid')
            if not workflow_task_id:
                raise ActionInvalid('workflow_task_id invalid')
            self.detail = {
                'table_id': context.table_id,
                'workflow_token': workflow_token,
                'workflow_name': workflow_name,
                'workflow_task_id': workflow_task_id,
                'row_id': converted_row['_id'] if converted_row else None
            }
        else:
            raise ActionInvalid()

    def get_users(self):
        result_users = []
        result_users.extend(self.users or [])
        if self.converted_row and self.users_column_key in self.converted_row:
            users_cell_value = self.converted_row[self.users_column_key]
            if isinstance(users_cell_value, list):
                result_users.extend(users_cell_value)
            elif isinstance(users_cell_value, str):
                result_users.append(users_cell_value)
        return [user for user in set(result_users) if is_valid_username(user)]

    def do_action(self):
        if not self.users and not self.users_column:
            return

        try:
            self.detail['msg'] = self.generate_real_msg(self.msg, self.converted_row)
            users = self.get_users()
            user_msg_list = []
            for user in users:
                user_msg_list.append({
                    'to_user': user,
                    'msg_type': self.MSG_TYPES_DICT[self.notify_type],
                    'detail': self.detail
                })
            send_notification(self.context.dtable_uuid, user_msg_list, self.context.access_token)
        except Exception as e:
            logger.exception(e)
            logger.error('msg detail: %s send users: %s notifications error: %s', self.detail, users, e)


class SendEmailAction(BaseAction):

    SEND_FROM_AUTOMATION_RULES = 'automation-rules'
    SEND_FROM_WORKFLOW = 'workflow'

    def __init__(self, context: BaseContext, account_id, subject, msg, send_to, copy_to, send_from, converted_row=None):
        super().__init__(context)
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        self.msg = msg
        self.converted_row = converted_row
        self.send_to_list = [email for email in email2list(send_to) if is_valid_username(email)]
        self.copy_to_list = [email for email in email2list(copy_to) if is_valid_username(email)]
        self.send_from = send_from
        self.subject = subject

    def do_action(self):
        if not self.account_dict:
            return
        account_detail = self.account_dict.get('detail', {})
        auth_info = {
            'email_host': account_detail.get('email_host', ''),
            'email_port': int(account_detail.get('email_port', 0)),
            'host_user': account_detail.get('host_user', ''),
            'password': account_detail.get('password', '')
        }
        send_info = {
            'send_to': self.send_to_list,
            'copy_to': self.copy_to_list,
            'subject': self.subject
        }
        try:
            send_info['message'] = self.generate_real_msg(self.msg, self.converted_row)
            send_email_msg(
                auth_info=auth_info,
                send_info=send_info,
                username=self.send_from,
                db_session=self.context.db_session
            )
        except Exception as e:
            logger.exception(e)
            logger.error('send email error: %s send_info: %s', e, self.send_info)


class SendWechatAction(BaseAction):

    def __init__(self, context: BaseContext, account_id, msg, msg_type, converted_row=None):
        super().__init__(context)
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        self.msg = msg
        self.msg_type = msg_type
        self.converted_row = converted_row

    def do_action(self):
        if not self.account_dict:
            return
        webhook_url = self.account_dict.get('detail', {}).get('webhook_url', '')
        if not webhook_url:
            logger.warning('account: %s no webhook_url', self.account_dict)
            return
        try:
            real_msg = self.generate_real_msg(self.msg, self.converted_row)
            send_wechat_msg(webhook_url, real_msg, self.msg_type)
        except Exception as e:
            logger.exception(e)
            logger.error('account: %s send wechat message error: %s', self.account_dict, e)


class SendDingtalkAction(BaseAction):

    def __init__(self, context: BaseContext, account_id, msg, msg_type, msg_title, converted_row=None):
        super().__init__(context)
        self.msg = msg
        self.msg_type = msg_type
        self.msg_title = msg_title
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        self.converted_row = converted_row

    def do_action(self):
        if not self.account_dict:
            return
        webhook_url = self.account_dict.get('detail', {}).get('webhook_url', '')
        if not webhook_url:
            return
        try:
            real_msg = self.generate_real_msg(self.msg, self.converted_row)
            send_dingtalk_msg(webhook_url, real_msg, self.msg_type, self.msg_title)
        except Exception as e:
            logger.exception(e)
            logger.error('account: %s send dingtalk message error: %s', self.account_dict, e)


class AddRowAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, context: BaseContext, new_row):
        super().__init__(context)
        if not new_row:
            raise ActionInvalid('new_row invalid')
        self.row = self.generate_filter_updates(new_row, self.context.table)

    def do_action(self):
        try:
            self.context.dtable_server_api.append_row(self.table['name'], self.row)
        except Exception as e:
            logger.error('add row dtable: %s error: %s', self.context.dtable_uuid, e)


class UpdateAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, context: BaseContext, updates, row_id):
        super().__init__(context)
        self.row_id = row_id
        self.row_data = self.generate_filter_updates(updates, self.context.table)

    def do_action(self):
        logger.debug('update dtable: %s row_id: %s self.row_data: %s', self.context.dtable_uuid, self.row_id, self.row_data)
        if not self.row_data:
            return
        try:
            self.context.dtable_server_api.update_row(self.table['name'], self.row_id, self.row_data)
        except Exception as e:
            logger.error('update dtable: %s error: %s', self.context.dtable_uuid, e)

class LockRecordAction(BaseAction):


    def __init__(self, context: BaseContext, row_id=None, filters=None, filter_conjunction=None):
        super().__init__(context)
        if not row_id and not self.context.view:
            raise ActionInvalid('row_id invalid or view: %s not found' % self.context.view_id)
        if not row_id and not all(filters, filter_conjunction):
            raise ActionInvalid('row_id or (filters, filter_conjunction) invalid')
        self.row_ids = []
        if row_id:
            self.row_ids.append(row_id)
        else:
            view_filters = self.context.view.get('filters', [])
            view_filter_conjunction = self.context.view.get('filter_conjunction', 'And')
            filter_groups = []
            if view_filters:
                filter_groups.append({
                    'filters': view_filters,
                    'filter_conjunction': view_filter_conjunction
                })
            condition_filters = [tmp_filters for tmp_filters in filters if filters not in view_filters]
            if condition_filters:
                filter_groups.append({
                    'filters': condition_filters,
                    'filter_conjunction': filter_conjunction
                })
            data = {
                'table_id': self.context.table_id,
                'filter_conditions': {
                    'filter_groups': filter_groups,
                    'group_conjunction': 'And'
                },
                'limit': 500
            }
            try:
                response = self.context.dtable_server_api.internal_filter_rows(data)
                rows_data = response['rows']
                logger.debug('Number of locking dtable row: %s, dtable_uuid: %s, details: %s' % (
                    len(rows_data),
                    self.context.dtable_uuid,
                    json.dumps(data)
                ))
                self.row_ids.extend([row['_id'] for row in rows_data])
            except Exception as e:
                logger.error('filter dtable: %s table: %s data: %s error: %s', self.context.dtable_uuid, self.context.table_id, data, e)

    def do_action(self):
        if not self.row_ids:
            return
        try:
            self.context.dtable_server_api.lock_rows(self.table['name'], self.row_ids)
        except Exception as e:
            logger.error('lock dtable: %s table: %s rows error: %s', self.context.dtable_uuid, self.context.table_id, e)


class LinkRecordsAction(BaseAction):

    COLUMN_FILTER_PREDICATE_MAPPING = {
        ColumnTypes.TEXT: "is",
        ColumnTypes.DATE: "is",
        ColumnTypes.LONG_TEXT: "is",
        ColumnTypes.CHECKBOX: "is",
        ColumnTypes.SINGLE_SELECT: "is",
        ColumnTypes.MULTIPLE_SELECT: "is_exactly",
        ColumnTypes.URL: "is",
        ColumnTypes.DURATION: "equal",
        ColumnTypes.NUMBER: "equal",
        ColumnTypes.COLLABORATOR: "is_exactly",
        ColumnTypes.EMAIL: "is",
        ColumnTypes.RATE: "equal",
    }

    def __init__(self, context: BaseContext, link_id, linked_table_id, match_conditions, converted_row):
        super().__init__(context)
        self.converted_row = converted_row
        self.link_id = link_id
        self.linked_table_id = linked_table_id
        self.linked_table_row_ids = []
        filter_groups = self._format_filter_groups(match_conditions, linked_table_id, converted_row)
        if filter_groups:
            json_data = {
                'table_id': linked_table_id,
                'filter_conditions': {
                    'filter_groups': filter_groups,
                    'group_conjunction': 'And'
                },
                'limit': 500
            }
            try:
                response = self.context.dtable_server_api.internal_filter_rows(json_data)
                rows_data = response['rows']
                logger.debug('Number of dtable link records filter rows: %s, dtable_uuid: %s, details: %s' % (
                    len(rows_data),
                    self.context.dtable_uuid,
                    json.dumps(json_data)
                ))
                self.linked_table_row_ids.extend([row['_id'] for row in rows_data])
            except Exception as e:
                logger.error('filter dtable: %s data: %s error: %s', self.context.dtable_uuid, json_data, e)

    def parse_column_value_back(self, column, value):
        if column.get('type') == ColumnTypes.SINGLE_SELECT:
            select_options = column.get('data', {}).get('options', [])
            for option in select_options:
                if value == option.get('name'):
                    return option.get('id')

        elif column.get('type') == ColumnTypes.MULTIPLE_SELECT:
            m_select_options = column.get('data', {}).get('options', [])
            if isinstance(value, list):
                parse_value_list = []
                for option in m_select_options:
                    if option.get('name') in value:
                        option_id = option.get('id')
                        parse_value_list.append(option_id)
                return parse_value_list
        elif column.get('type') in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            return [value]
        else:
            return value

    def _format_filter_groups(self, match_conditions, linked_table_id, converted_row):
        filters = []
        for match_condition in match_conditions:
            column_key = match_condition.get('column_key')
            column = self.context.columns_dict.get(column_key)
            if not column:
                return []
            row_value = converted_row.get(column['name'])
            if not row_value:
                return []
            other_column_key = match_condition.get('other_column_key')
            other_column = self.context.get_table_column_by_key(linked_table_id, other_column_key)
            if not other_column:
                continue
            parsed_row_value = self.parse_column_value_back(other_column, row_value)
            filter_item = {
                'column_key': other_column_key,
                'filter_predicate': self.COLUMN_FILTER_PREDICATE_MAPPING.get(other_column['type'], 'is'),
                'filter_term': parsed_row_value,
                'filter_term_modifier': 'exact_date'
            }
            filters.append(filter_item)
        return filters and [{'filters': filters, 'filter_conjunction': 'And'}] or []

    def do_action(self):
        if not self.linked_table_row_ids:
            return
        try:
            self.context.dtable_server_api.update_link(self.link_id, self.context.table_id, self.linked_table_id, self.converted_row['_id'], self.linked_table_row_ids)
        except:
            logger.error('link dtable: %s error: %s', self.context.dtable_uuid, e)


class RunPythonScriptAction(BaseAction):

    OPERATE_FROM_AUTOMATION_RULE = 'automation-rule'
    OPERATE_FROM_WORKFLOW = 'workflow'

    def __init__(self, context: BaseContext, script_name, workspace_id, owner, org_id, repo_id,
                converted_row=None, operate_from=None, operator=None):
        super().__init__(context)
        self.script_name = script_name
        self.workspace_id = workspace_id
        self.owner = owner
        self.org_id = org_id
        self.repo_id = repo_id
        self.converted_row = converted_row
        self.operate_from = operate_from
        self.operator = operator

    def _can_do_action(self):
        if not SEATABLE_FAAS_URL:
            return False
        if self.context.can_run_python is not None:
            return self.context.can_run_python
        permission_url = DTABLE_WEB_SERVICE_URL.strip('/') + '/api/v2.1/script-permissions/'
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        if self.org_id != -1:
            json_data = {'org_ids': [self.org_id]}
        elif self.org_id == -1 and '@seafile_group' not in self.owner:
            json_data = {'users': [self.owner]}
        else:
            return True
        try:
            resp = requests.get(permission_url, headers=headers, json=json_data)
            if resp.status_code != 200:
                logger.error('check run script permission error response: %s', resp.status_code)
                return False
            permission_dict = resp.json()
        except Exception as e:
            logger.error('check run script permission error: %s', e)
            return False

        # response dict like
        # {
        #   'user_script_permissions': {username1: {'can_run_python_script': True/False}}
        #   'can_schedule_run_script': {org1: {'can_run_python_script': True/False}}
        # }
        if self.org_id != -1:
            can_run_python = permission_dict['org_script_permissions'][str(self.org_id)]['can_run_python_script']
        else:
            can_run_python = permission_dict['user_script_permissions'][self.owner]['can_run_python_script']

        self.context.can_run_python = can_run_python
        return can_run_python

    def _get_scripts_running_limit(self):
        if self.context.scripts_running_limit is not None:
            return self.context.scripts_running_limit
        if self.org_id != -1:
            params = {'org_id': self.org_id}
        elif self.org_id == -1 and '@seafile_group' not in self.owner:
            params = {'username': self.owner}
        else:
            return -1
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/api/v2.1/scripts-running-limit/'
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        try:
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                logger.error('get scripts running limit error response: %s', resp.status_code)
                return 0
            scripts_running_limit = resp.json()['scripts_running_limit']
        except Exception as e:
            logger.error('get script running limit error: %s', e)
            return 0
        self.context.scripts_running_limit = scripts_running_limit
        return scripts_running_limit

    def do_action(self):
        if not self._can_do_action():
            return
        context_data = {
            'table': self.context.table['name']
        }
        if self.converted_row:
            context_data['row'] = self.converted_row
        scripts_running_limit = self._get_scripts_running_limit()

        # request faas url
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        try:
            resp = requests.post(self.RUN_SCRIPT_URL, json={
                'dtable_uuid': str(UUID(self.context.dtable_uuid)),
                'script_name': self.script_name,
                'context_data': context_data,
                'owner': self.owner,
                'org_id': self.org_id,
                'temp_api_token': self.context.get_temp_api_token(app_name=self.script_name),
                'scripts_running_limit': scripts_running_limit,
                'operate_from': self.operate_from,
                'operator': self.operator
            }, headers=headers, timeout=10)
            if resp.status_code != 200:
                logger.error('dtable: %s run script: %s error status code: %s', self.context.dtable_uuid, self.script_name, resp.status_code)
        except Exception as e:
            logger.error('dtable: %s run script: %s error: %s', self.context.dtable_uuid, self.script_name, e)


class AddRecordToOtherTableAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, context: BaseContext, dst_table_id, new_row):
        super().__init__(context)
        if not new_row:
            raise ActionInvalid('new_row invalid')
        dst_table = self.context.get_table_by_id(dst_table_id)
        if not dst_table:
            raise ActionInvalid('dtable: %s table: %s not found' % (self.context.dtable_uuid, dst_table_id))
        self.dst_table = dst_table
        self.row = self.generate_filter_updates(new_row, dst_table)

    def do_action(self):
        try:
            self.context.dtable_server_api.append_row(self.dst_table['name'], self.row)
        except Exception as e:
            logger.error('add row dtable: %s error: %s', self.context.dtable_uuid, e)
