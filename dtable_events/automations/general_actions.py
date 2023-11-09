import json
import logging
import re
import time
from copy import deepcopy
from uuid import UUID
from datetime import datetime, timedelta

import jwt
import requests

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, SEATABLE_FAAS_AUTH_TOKEN, \
    SEATABLE_FAAS_URL, INNER_DTABLE_DB_URL
from dtable_events.automations.models import BoundThirdPartyAccounts
from dtable_events.dtable_io import send_wechat_msg, send_email_msg, send_dingtalk_msg
from dtable_events.notification_rules.notification_rules_utils import fill_msg_blanks_with_converted_row, \
    send_notification
from dtable_events.utils import is_valid_email, get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import DTableServerAPI, NotFoundException
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.sql_generator import filter2sql


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


class MetadataInvalid(ContextInvalid):
    pass


class TableInvalid(ContextInvalid):
    pass


class ViewInvalid(ContextInvalid):
    pass


class BaseContext:

    def __init__(self, dtable_uuid, table_id, db_session, view_id=None, caller='dtable-events'):
        self.dtable_uuid = str(UUID(dtable_uuid))
        self.table_id = table_id
        self.view_id = view_id
        self.db_session = db_session
        self.caller = caller

        self.dtable_server_api = DTableServerAPI(caller, self.dtable_uuid, get_inner_dtable_server_url())
        self.dtable_db_api = DTableDBAPI(caller, self.dtable_uuid, INNER_DTABLE_DB_URL)
        self.dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)

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

        # load metadata table and view
        self.get_dtable_resources()

    def get_dtable_resources(self):
        return {
            'dtable_metadata': self.dtable_metadata,
            'table': self.table,
            'view': self.view,
            'related_users': self.related_users
        }

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
        try:
            self._dtable_metadata = self.dtable_server_api.get_metadata()
        except NotFoundException:
            raise MetadataInvalid('dtable: %s metadata not found' % self.dtable_uuid)
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
            raise TableInvalid('dtable: %s self.table: %s not found' % (self.dtable_uuid, self.table_id))
        return self._table

    @property
    def columns_dict(self):
        if self._columns_dict:
            return self._columns_dict
        self._columns_dict = {col['key']: col for col in self.table['columns']}
        return self._columns_dict

    @property
    def view(self):
        if not self.view_id:
            return None
        if self._view:
            return self._view
        if not self.view_id:
            return None
        for view in self.table['views']:
            if view['_id'] == self.view_id:
                self._view = view
                break
        if not self._view:
            raise ViewInvalid('dtable: %s self.table: %s self.view: %s not found' % (self.dtable_uuid, self.table_id, self.view_id))
        return self._view

    @property
    def related_users(self):
        if not self._related_users:
            self._related_users = self.dtable_web_api.get_related_users(self.dtable_uuid, self.caller)
        return self._related_users

    @property
    def related_users_dict(self):
        if not self._related_users_dict:
            self._related_users_dict = {user['email']: user for user in self.related_users}
        return self._related_users_dict

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
        temp_api_token = jwt.encode(payload, SEATABLE_FAAS_AUTH_TOKEN, algorithm='HS256')
        return temp_api_token

    def get_converted_row(self, table_id, row_id):
        table = self.get_table_by_id(table_id)
        logger.debug('table_id: %s table_name: %s row_id: %s', table and table['name'], table_id, row_id)
        if not table:
            logger.error('dtable: %s table: %s not found', self.dtable_uuid, table_id)
            return None
        try:
            converted_row = self.dtable_server_api.get_row(table['name'], row_id, convert_link_id=True)
            if not converted_row:
                logger.error('dtable: %s table: %s row: %s not found or parse error', self.dtable_uuid, table_id, row_id)
                return None
        except Exception as e:
            logger.error('dtable: %s table: %s row: %s error: %s', self.dtable_uuid, table_id, row_id, e)
            return None
        return converted_row


class ActionInvalid(Exception):
    pass


class RelatedUserInvalid(ActionInvalid):
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
        try:
            return fill_msg_blanks_with_converted_row(msg, column_blanks, col_name_dict, converted_row, self.context.db_session)
        except Exception as e:
            logger.exception(e)
            logger.error('msg: %s col_name_dict: %s column_blanks: %s fill error: %s', msg, col_name_dict, column_blanks, e)
            return msg

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
                    except Exception as e:
                        logger.error(e)
                        filter_updates[col_name] = add_or_updates.get(col_key)
                else:
                    filter_updates[col_name] = parse_column_value(col, add_or_updates.get(col_key))
        return filter_updates


class NotifyAction(BaseAction):

    NOTIFY_TYPE_NOTIFICATION_RULE = 'notify_type_notification_rule'
    NOTIFY_TYPE_AUTOMATION_RULE = 'notify_type_automation_rule'
    NOTIFY_TYPE_WORKFLOW = 'notify_type_workflow'

    MSG_TYPES_DICT = {
        NOTIFY_TYPE_NOTIFICATION_RULE: 'notification_rules',
        NOTIFY_TYPE_AUTOMATION_RULE: 'notification_rules',
        NOTIFY_TYPE_WORKFLOW: 'workflows'
    }

    def __init__(self, context: BaseContext, users, msg, notify_type, users_column_key=None,
                condition=None, rule_id=None, rule_name=None,
                workflow_token=None, workflow_name=None, workflow_task_id=None):
        super().__init__(context)
        self.users = users
        self.notify_type = notify_type
        self.users_column_key = users_column_key
        self.users_column = self.context.columns_dict.get(self.users_column_key)
        self.msg = msg
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
                'rule_name': rule_name
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
                'rule_name': rule_name
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
                'workflow_task_id': workflow_task_id
            }
        else:
            raise ActionInvalid('notify_type: %s invalid' % notify_type)

        self.validate_users()

    def validate_users(self):
        users = []
        for user in self.users:
            if user in self.context.related_users_dict:
                users.append(user)
                continue
            if self.notify_type in [self.NOTIFY_TYPE_AUTOMATION_RULE, self.NOTIFY_TYPE_NOTIFICATION_RULE]:
                raise RelatedUserInvalid('user %s not in %s related users' % (user, self.context.dtable_uuid))
        self.users = users

    def get_users(self, converted_row):
        result_users = []
        result_users.extend(self.users or [])
        if converted_row and self.users_column and self.users_column['name'] in converted_row:
            users_cell_value = converted_row[self.users_column['name']]
            if isinstance(users_cell_value, list):
                result_users.extend(users_cell_value)
            elif isinstance(users_cell_value, str):
                result_users.append(users_cell_value)
        return [user for user in set(result_users) if is_valid_username(user) and user in self.context.related_users_dict]

    def send_to_users(self, to_users, detail, msg_type):
        user_msg_list = []
        for user in to_users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': msg_type,
                'detail': detail
            })
        try:
            send_notification(self.context.dtable_uuid, user_msg_list, self.context.access_token)
        except Exception as e:
            logger.exception(e)
            logger.error('msg detail: %s send users: %s notifications error: %s', detail, to_users, e)

    def do_action_without_row(self):
        if not self.users and not self.users_column:
            return
        detail = deepcopy(self.detail)
        detail['msg'] = self.msg
        self.send_to_users(self.users, detail, self.MSG_TYPES_DICT[self.notify_type])

    def do_action_with_row(self, converted_row):
        if not self.users and not self.users_column:
            return
        users = self.get_users(converted_row)
        detail = deepcopy(self.detail)
        if self.notify_type in [self.NOTIFY_TYPE_AUTOMATION_RULE, self.NOTIFY_TYPE_NOTIFICATION_RULE]:
            detail['row_id_list'] = [converted_row['_id']]
        elif self.notify_type == self.NOTIFY_TYPE_WORKFLOW:
            detail['row_id'] = converted_row['_id']
        detail['msg'] = self.generate_real_msg(self.msg, converted_row)
        self.send_to_users(users, detail, self.MSG_TYPES_DICT[self.notify_type])


class SendEmailAction(BaseAction):

    SEND_FROM_AUTOMATION_RULES = 'automation-rules'
    SEND_FROM_WORKFLOW = 'workflow'

    def __init__(self, context: BaseContext, account_id, subject, msg, send_to, copy_to, send_from):
        super().__init__(context)
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        if not self.account_dict:
            raise ActionInvalid('account_id: %s not found' % account_id)
        self.msg = msg
        self.send_to_list = email2list(send_to)
        self.copy_to_list = email2list(copy_to)
        self.send_from = send_from
        self.subject = subject

    def do_action_without_row(self):
        return self.do_action_with_row(None)

    def do_action_with_row(self, converted_row):
        final_send_to_list, final_copy_to_list = [], []
        if self.send_to_list:
            for send_to in self.send_to_list:
                real_send_to = self.generate_real_msg(send_to, converted_row)
                if is_valid_email(real_send_to):
                    final_send_to_list.append(real_send_to)
        if not final_send_to_list:
            return
        if self.copy_to_list:
            for copy_to in self.copy_to_list:
                real_copy_to = self.generate_real_msg(copy_to, converted_row)
                if is_valid_email(real_copy_to):
                    final_copy_to_list.append(real_copy_to)
        account_detail = self.account_dict.get('detail', {})
        auth_info = {
            'email_host': account_detail.get('email_host', ''),
            'email_port': int(account_detail.get('email_port', 0)),
            'host_user': account_detail.get('host_user', ''),
            'password': account_detail.get('password', '')
        }
        send_info = {
            'send_to': final_send_to_list,
            'copy_to': final_copy_to_list,
            'subject': self.subject
        }
        try:
            send_info['message'] = self.generate_real_msg(self.msg, converted_row)
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

    def __init__(self, context: BaseContext, account_id, msg, msg_type):
        super().__init__(context)
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        if not self.account_dict:
            raise ActionInvalid('account_id: %s not found' % account_id)
        self.msg = msg
        self.msg_type = msg_type

    def do_action_without_row(self):
        return self.do_action_with_row(None)

    def do_action_with_row(self, converted_row):
        webhook_url = self.account_dict.get('detail', {}).get('webhook_url', '')
        if not webhook_url:
            logger.warning('account: %s no webhook_url', self.account_dict)
            return
        try:
            real_msg = self.generate_real_msg(self.msg, converted_row)
            send_wechat_msg(webhook_url, real_msg, self.msg_type)
        except Exception as e:
            logger.exception(e)
            logger.error('account: %s send wechat message error: %s', self.account_dict, e)


class SendDingtalkAction(BaseAction):

    def __init__(self, context: BaseContext, account_id, msg, msg_type, msg_title):
        super().__init__(context)
        self.msg = msg
        self.msg_type = msg_type
        self.msg_title = msg_title
        self.account_dict = get_third_party_account(self.context.db_session, account_id)
        if not self.account_dict:
            raise ActionInvalid('account_id: %s not found' % account_id)

    def do_action_without_row(self):
        return self.do_action_with_row(None)

    def do_action_with_row(self, converted_row):
        webhook_url = self.account_dict.get('detail', {}).get('webhook_url', '')
        if not webhook_url:
            return
        try:
            real_msg = self.generate_real_msg(self.msg, converted_row)
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

    def do_action_without_row(self):
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

    def __init__(self, context: BaseContext, updates):
        super().__init__(context)
        self.row_data = self.generate_filter_updates(updates, self.context.table)

    def do_action_with_row(self, converted_row):
        row_id = converted_row['_id']
        logger.debug('update dtable: %s row_id: %s self.row_data: %s', self.context.dtable_uuid, row_id, self.row_data)
        if not self.row_data:
            return
        try:
            self.context.dtable_server_api.update_row(self.table['name'], row_id, self.row_data)
        except Exception as e:
            logger.error('update dtable: %s error: %s', self.context.dtable_uuid, e)


class LockRecordAction(BaseAction):

    def __init__(self, context: BaseContext):
        super().__init__(context)

    def do_action_with_row_ids(self, row_ids):
        try:
            self.context.dtable_server_api.lock_rows(self.table['name'], row_ids)
        except Exception as e:
            logger.error('lock dtable: %s table: %s rows error: %s', self.context.dtable_uuid, self.context.table_id, e)

    def do_action_with_row(self, converted_row):
        self.do_action_with_row_ids([converted_row['_id']])


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

    def __init__(self, context: BaseContext, link_id, linked_table_id, match_conditions):
        super().__init__(context)
        self.link_id = link_id
        self.linked_table_id = linked_table_id
        self.match_conditions = match_conditions

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

    def do_action_with_row(self, converted_row):
        linked_table_row_ids = []
        filter_groups = self._format_filter_groups(self.match_conditions, self.linked_table_id, converted_row)
        if filter_groups:
            filter_conditions = {
                'filter_groups': filter_groups,
                'group_conjunction': 'And',
                'start': 0,
                'limit': 500,
            }
            linked_table = self.context.get_table_by_id(self.linked_table_id)

            table_name = linked_table.get('name')
            columns = linked_table.get('columns')

            sql = filter2sql(table_name, columns, filter_conditions, by_group=True)

            try:
                rows_data, _ = self.context.dtable_db_api.query(sql, convert=False)
                logger.debug('Number of dtable link records filter rows: %s, dtable_uuid: %s, details: %s' % (
                    len(rows_data),
                    self.context.dtable_uuid,
                    json.dumps(filter_conditions)
                ))
                linked_table_row_ids.extend([row['_id'] for row in rows_data])
            except Exception as e:
                logger.error('filter dtable: %s data: %s error: %s', self.context.dtable_uuid, filter_conditions, e)
                return
        if not linked_table_row_ids:
            return
        try:
            self.context.dtable_server_api.update_link(self.link_id, self.context.table_id, self.linked_table_id, converted_row['_id'], linked_table_row_ids)
        except Exception as e:
            logger.error('link dtable: %s error: %s', self.context.dtable_uuid, e)


class RunPythonScriptAction(BaseAction):

    OPERATE_FROM_AUTOMATION_RULE = 'automation-rule'
    OPERATE_FROM_WORKFLOW = 'workflow'

    def __init__(self, context: BaseContext, script_name, workspace_id, owner, org_id, repo_id,
                 operate_from=None, operator=None):
        super().__init__(context)
        self.script_name = script_name
        self.workspace_id = workspace_id
        self.owner = owner
        self.org_id = org_id
        self.repo_id = repo_id
        self.operate_from = operate_from
        self.operator = operator

    def can_run_python(self):
        if not SEATABLE_FAAS_URL:
            return False
        if self.context.can_run_python is not None:
            return self.context.can_run_python

        if self.org_id != -1:
            can_run_python = self.context.can_run_python = self.context.dtable_web_api.can_org_run_python(self.org_id)
        else:
            can_run_python = self.context.can_run_python = self.context.dtable_web_api.can_user_run_python(self.owner)

        return can_run_python

    def get_scripts_running_limit(self):
        if self.context.scripts_running_limit is not None:
            return self.context.scripts_running_limit

        if self.org_id != -1:
            scripts_running_limit = self.context.scripts_running_limit = self.context.dtable_web_api.get_org_scripts_running_limit(self.org_id)
        else:
            scripts_running_limit = self.context.scripts_running_limit = self.context.dtable_web_api.get_user_scripts_running_limit(self.owner)

        return scripts_running_limit

    def do_action_without_row(self):
        return self.do_action_with_row(None)

    def do_action_with_row(self, converted_row):
        if not self.can_run_python():
            return
        context_data = {
            'table': self.context.table['name']
        }
        if converted_row:
            context_data['row'] = converted_row
        scripts_running_limit = self.get_scripts_running_limit()

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
                logger.warning('dtable: %s run script: %s error status code: %s content: %s', self.context.dtable_uuid, self.script_name, resp.status_code, resp.content)
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

    def do_action_without_row(self):
        try:
            self.context.dtable_server_api.append_row(self.dst_table['name'], self.row)
        except Exception as e:
            logger.error('add row dtable: %s error: %s', self.context.dtable_uuid, e)
