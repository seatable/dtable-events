import json
import logging
import re
import time
import os
from urllib import parse
from uuid import UUID
from datetime import datetime, date, timedelta

import jwt
import requests

from dtable_events.automations.models import BoundThirdPartyAccounts
from dtable_events.cache import redis_cache
from dtable_events.dtable_io import send_wechat_msg, send_email_msg, send_dingtalk_msg
from dtable_events.notification_rules.notification_rules_utils import _fill_msg_blanks as fill_msg_blanks, \
    send_notification
from dtable_events.utils import utc_to_tz, uuid_str_to_36_chars, is_valid_email, get_inner_dtable_server_url
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import DTableServerAPI


logger = logging.getLogger(__name__)

# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')
try:
    import seahub.settings as seahub_settings
    DTABLE_WEB_SERVICE_URL = getattr(seahub_settings, 'DTABLE_WEB_SERVICE_URL')
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
    FILE_SERVER_ROOT = getattr(seahub_settings, 'FILE_SERVER_ROOT', 'http://127.0.0.1:8082')
    SEATABLE_FAAS_AUTH_TOKEN = getattr(seahub_settings, 'SEATABLE_FAAS_AUTH_TOKEN')
    SEATABLE_FAAS_URL = getattr(seahub_settings, 'SEATABLE_FAAS_URL')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

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

MINUTE_TIMEOUT = 60

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


class BaseAction:

    def __init__(self, auto_rule, data=None):
        self.auto_rule = auto_rule
        self.action_type = 'base'
        self.data = data

    def do_action(self):
        pass

    def parse_column_value(self, column, value):
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

    def __init__(self, auto_rule, data, updates):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, data)
        self.action_type = 'update'
        self.updates = updates
        self.update_data = {
            'row': {},
            'table_name': self.auto_rule.table_info['name'],
            'row_id': ''
        }
        self.col_name_dict = {}
        self._init_updates()

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def _fill_msg_blanks(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, text, blanks, col_name_dict, row, db_session, dtable_metadata)

    def _init_updates(self):
        src_row = self.data['converted_row']
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}

        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        if self.auto_rule.run_condition == PER_UPDATE:
            for col in self.auto_rule.table_info['columns']:
                if 'key' in col and col.get('type') in self.VALID_COLUMN_TYPES:
                    col_name = col.get('name')
                    col_key = col.get('key')
                    col_type = col.get('type')
                    if col_key in self.updates.keys():
                        if col_type == ColumnTypes.DATE:
                            time_format = col.get('data', {}).get('format', '')
                            format_length = len(time_format.split(" "))
                            try:
                                time_dict = self.updates.get(col_key)
                                set_type = time_dict.get('set_type')
                                if set_type == 'specific_value':
                                    time_value = time_dict.get('value')
                                    filtered_updates[col_name] = time_value
                                elif set_type == 'relative_date':
                                    offset = time_dict.get('offset')
                                    filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                            except Exception as e:
                                logger.error(e)
                                filtered_updates[col_name] = self.updates.get(col_key)
                        else:
                            cell_value = self.updates.get(col_key)
                            if isinstance(cell_value, str):
                                blanks = set(re.findall(r'\{([^{]*?)\}', cell_value))
                                column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
                                cell_value = self._fill_msg_blanks(src_row, cell_value, column_blanks)
                            filtered_updates[col_name] = self.parse_column_value(col, cell_value)
            row_id = self.data['row']['_id']
            self.update_data['row'] = filtered_updates
            self.update_data['row_id'] = row_id

    def _can_do_action(self):
        if not self.update_data.get('row') or not self.update_data.get('row_id'):
            return False
        if self.auto_rule.run_condition == PER_UPDATE:
            # if columns in self.updates was updated, forbidden action!!!
            updated_column_keys = self.data.get('updated_column_keys', [])
            to_update_keys = [col['key'] for col in self.auto_rule.table_info['columns'] if col['name'] in self.updates]
            for key in updated_column_keys:
                if key in to_update_keys:
                    return False
        if self.auto_rule.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        try:
            self.auto_rule.dtable_server_api.update_row(self.auto_rule.table_info['name'], self.data['row']['_id'], self.update_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()

class LockRowAction(BaseAction):


    def __init__(self, auto_rule, data, trigger):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, data)
        self.action_type = 'lock'
        self.update_data = {
            'table_name': self.auto_rule.table_info['name'],
            'row_ids':[],
        }
        self.trigger = trigger
        self._init_updates()

    def _check_row_conditions(self):
        filters = self.trigger.get('filters', [])
        filter_conjunction = self.trigger.get('filter_conjunction', 'And')
        table_id = self.auto_rule.table_id
        view_info = self.auto_rule.view_info
        view_filters = view_info.get('filters', [])
        view_filter_conjunction = view_info.get('filter_conjunction', 'And')
        filter_groups = []

        if view_filters:
            filter_groups.append({'filters': view_filters, 'filter_conjunction': view_filter_conjunction})

        if filters:
            # remove the duplicate filter which may already exist in view filter
            trigger_filters = [trigger_filter for trigger_filter in filters if trigger_filter not in view_filters]
            if trigger_filters:
                filter_groups.append({'filters': trigger_filters, 'filter_conjunction': filter_conjunction})

        json_data = {
            'table_id': table_id,
            'filter_conditions': {
                'filter_groups':filter_groups,
                'group_conjunction': 'And'
            },
            'limit': 500
        }
        try:
            response_data = self.auto_rule.dtable_server_api.internal_filter_rows(json_data)
            rows_data = response_data.get('rows')
            logger.debug('Number of locking dtable row by auto-rules: %s, dtable_uuid: %s, details: %s' % (
                len(rows_data),
                self.auto_rule.dtable_uuid,
                json.dumps(json_data)
            ))
            return rows_data or []
        except Exception as e:
            logger.error('lock dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return []

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        if self.auto_rule.run_condition == PER_UPDATE:
            row_id = self.data['row']['_id']
            self.update_data['row_ids'].append(row_id)

        if self.auto_rule.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            rows_data = self._check_row_conditions()
            for row in rows_data:
                self.update_data['row_ids'].append(row.get('_id'))

    def _can_do_action(self):
        if not self.update_data.get('row_ids'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return

        try:
            self.auto_rule.dtable_server_api.lock_rows(self.auto_rule.table_info['name'], self.update_data.get('row_ids'))
        except Exception as e:
            logger.error('lock dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()

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

    def __init__(self, auto_rule, row):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        row: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule)
        self.action_type = 'add'
        self.row = row
        self.row_data = {
            'row': {},
            'table_name': self.auto_rule.table_info['name']
        }
        self._init_updates()

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        for col in self.auto_rule.table_info['columns']:
            if 'key' in col and col.get('type') in self.VALID_COLUMN_TYPES:
                col_name = col.get('name')
                col_type = col.get('type')
                col_key = col.get('key')
                if col_key in self.row.keys():
                    if col_type == ColumnTypes.DATE:
                        time_format = col.get('data', {}).get('format', '')
                        format_length = len(time_format.split(" "))
                        try:
                            time_dict = self.row.get(col_key)
                            if not time_dict:
                                continue
                            set_type = time_dict.get('set_type')
                            if set_type == 'specific_value':
                                time_value = time_dict.get('value')
                                filtered_updates[col_name] = time_value
                            elif set_type == 'relative_date':
                                offset = time_dict.get('offset')
                                filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                        except Exception as e:
                            logger.error(e)
                            filtered_updates[col_name] = self.row.get(col_key)
                    else:
                        filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))
        self.row_data['row'] = filtered_updates

    def _can_do_action(self):
        if not self.row_data.get('row'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return
        try:
            self.auto_rule.dtable_server_api.append_row(self.auto_rule.table_info['name'], self.row_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()

class NotifyAction(BaseAction):

    def __init__(self, auto_rule, data, msg, users, users_column_key):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        msg: message set in action
        users: who will receive notification(s)
        """
        super().__init__(auto_rule, data)
        self.action_type = 'notify'
        self.msg = msg
        self.users = users
        self.users_column_key = users_column_key

        self.column_blanks = []
        self.col_name_dict = {}


        self._init_notify(msg)

    def is_valid_username(self, user):
        if not user:
            return False

        return is_valid_email(user)

    def get_user_column_by_key(self):
        dtable_metadata = self.auto_rule.dtable_metadata
        table = None
        for t in dtable_metadata.get('tables', []):
            if t.get('_id') == self.auto_rule.table_id:
                table = t
                break

        if not table:
            return None

        for col in table.get('columns'):
            if col.get('key') == self.users_column_key:
                return col

        return None

    def _init_notify(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        dtable_uuid, row, raw_row = self.auto_rule.dtable_uuid, self.data['converted_row'], self.data['row']
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        msg = self.msg
        if self.column_blanks:
            msg = self._fill_msg_blanks(row)

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': self.auto_rule.trigger.get('condition'),
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': msg,
            'row_id_list': [row['_id']],
        }

        user_msg_list = []
        users = self.users
        if self.users_column_key:
            user_column = self.get_user_column_by_key()
            if user_column:
                users_column_name = user_column.get('name')
                users_from_column = row.get(users_column_name, [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(self.users + users_from_column))
            else:
                logger.warning('automation rule: %s notify action user column: %s invalid', self.auto_rule.rule_id, self.users_column_key)
        for user in users:
            if not self.is_valid_username(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def cron_notify(self):
        dtable_uuid = self.auto_rule.dtable_uuid
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_PERIODICALLY,
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': self.msg,
            'row_id_list': []
        }
        user_msg_list = []
        for user in self.users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK, PER_MONTH]:
            self.cron_notify()
            self.auto_rule.set_done_actions()

class SendWechatAction(BaseAction):

    def __init__(self, auto_rule, data, msg, account_id, msg_type):

        super().__init__(auto_rule, data)
        self.action_type = 'send_wechat'
        self.msg = msg
        self.msg_type = msg_type
        self.account_id = account_id

        self.webhook_url = ''
        self.column_blanks = []
        self.col_name_dict = {}

        self._init_notify(msg)


    def _init_notify(self, msg):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            self.auto_rule.set_invalid()
            return
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
        self.webhook_url = account_dict.get('detail', {}).get('webhook_url', '')

    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.msg
        if self.column_blanks:
            msg = self._fill_msg_blanks(row)
        try:
            send_wechat_msg(self.webhook_url, msg, self.msg_type)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def cron_notify(self):
        try:
            send_wechat_msg(self.webhook_url, self.msg, self.msg_type)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()


class SendDingtalkAction(BaseAction):

    def __init__(self, auto_rule, data, msg, account_id, msg_type, msg_title):

        super().__init__(auto_rule, data)
        self.action_type = 'send_dingtalk'
        self.msg = msg
        self.msg_type = msg_type
        self.account_id = account_id
        self.msg_title = msg_title

        self.webhook_url = ''
        self.column_blanks = []
        self.col_name_dict = {}

        self._init_notify(msg)


    def _init_notify(self, msg):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            self.auto_rule.set_invalid()
            return
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
        self.webhook_url = account_dict.get('detail', {}).get('webhook_url', '')

    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.msg
        if self.column_blanks:
            msg = self._fill_msg_blanks(row)
        try:
            send_dingtalk_msg(self.webhook_url, msg, self.msg_type, self.msg_title)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def cron_notify(self):
        try:
            send_dingtalk_msg(self.webhook_url, self.msg, self.msg_type, self.msg_title)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()


class SendEmailAction(BaseAction):


    def is_valid_email(self, email):
        """A heavy email format validation.
        """
        return is_valid_email(email)

    def __init__(self,
                 auto_rule,
                 data,
                 send_info,
                 account_id,
                 ):

        super().__init__(auto_rule, data)
        self.action_type = 'send_email'
        self.account_id = account_id

        # send info
        self.send_info = send_info

        # auth info
        self.auth_info = {}


        self.column_blanks = []
        self.column_blanks_send_to = []
        self.column_blanks_copy_to = []
        self.col_name_dict = {}

        self._init_notify()

    def _init_notify_msg(self):
        msg = self.send_info.get('message')
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify_send_to(self):
        send_to_list = self.send_info.get('send_to')
        blanks = []
        for send_to in send_to_list:
            res = re.findall(r'\{([^{]*?)\}', send_to)
            if res:
                blanks.extend(res)
        self.column_blanks_send_to = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify_copy_to(self):
        copy_to_list = self.send_info.get('copy_to')
        blanks = []
        for copy_to in copy_to_list:
            res = re.findall(r'\{([^{]*?)\}', copy_to)
            if res:
                blanks.extend(res)
        self.column_blanks_copy_to = [blank for blank in blanks if blank in self.col_name_dict]

    def _init_notify(self):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            self.auto_rule.set_invalid()
            return

        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self._init_notify_msg()
        self._init_notify_send_to()
        self._init_notify_copy_to()

        account_detail = account_dict.get('detail', {})

        email_host = account_detail.get('email_host', '')
        email_port = account_detail.get('email_port', 0)
        host_user = account_detail.get('host_user', '')
        password = account_detail.get('password', '')
        self.auth_info = {
            'email_host': email_host,
            'email_port': int(email_port),
            'host_user': host_user,
            'password' : password
        }

    def _fill_msg_blanks(self, row, text, blanks):

        col_name_dict = self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, text, blanks, col_name_dict, row, db_session, dtable_metadata)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.send_info.get('message', '')
        send_to_list = self.send_info.get('send_to', [])
        copy_to_list = self.send_info.get('copy_to', [])
        if self.column_blanks:
            msg = self._fill_msg_blanks(row, msg, self.column_blanks)
        if self.column_blanks_send_to:
            send_to_list = [self._fill_msg_blanks(row, send_to, self.column_blanks_send_to) for send_to in send_to_list]
        if self.column_blanks_copy_to:
            copy_to_list = [self._fill_msg_blanks(row, copy_to, self.column_blanks_copy_to) for copy_to in copy_to_list]
        self.send_info.update({
            'message': msg,
            'send_to': [send_to for send_to in send_to_list if self.is_valid_email(send_to)],
            'copy_to': [copy_to for copy_to in copy_to_list if self.is_valid_email(copy_to)],
        })
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=self.send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def cron_notify(self):
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=self.send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()


class RunPythonScriptAction(BaseAction):

    def __init__(self, auto_rule, data, script_name, workspace_id, owner, org_id, repo_id):
        super().__init__(auto_rule, data=data)
        self.action_type = 'run_python_script'
        self.script_name = script_name
        self.workspace_id = workspace_id
        self.owner = owner
        self.org_id = org_id
        self.repo_id = repo_id

    def _can_do_action(self):
        if not SEATABLE_FAAS_URL:
            return False
        if self.auto_rule.can_run_python is not None:
            return self.auto_rule.can_run_python

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

        self.auto_rule.can_run_python = can_run_python
        return can_run_python

    def _get_scripts_running_limit(self):
        if self.auto_rule.scripts_running_limit is not None:
            return self.auto_rule.scripts_running_limit
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
        self.auto_rule.scripts_running_limit = scripts_running_limit
        return scripts_running_limit

    def do_action(self):
        if not self._can_do_action():
            return

        context_data = {'table': self.auto_rule.table_info['name']}
        if self.auto_rule.run_condition == PER_UPDATE:
            context_data['row'] = self.data['converted_row']
        scripts_running_limit = self._get_scripts_running_limit()

        # request faas url
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        url = SEATABLE_FAAS_URL.strip('/') + '/run-script/'
        try:
            response = requests.post(url, json={
                'dtable_uuid': str(UUID(self.auto_rule.dtable_uuid)),
                'script_name': self.script_name,
                'context_data': context_data,
                'owner': self.owner,
                'org_id': self.org_id,
                'temp_api_token': self.auto_rule.get_temp_api_token(app_name=self.script_name),
                'scripts_running_limit': scripts_running_limit,
                'operate_from': 'automation-rule',
                'operator': self.auto_rule.rule_id
            }, headers=headers, timeout=10)
        except Exception as e:
            logger.exception(e)
            logger.error(e)
        else:
            if response.status_code != 200:
                logger.warning('run script error status code: %s', response.status_code)
            else:
                self.auto_rule.set_done_actions()


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

    def __init__(self, auto_rule, data, linked_table_id, link_id, match_conditions):
        super().__init__(auto_rule, data=data)
        self.action_type = 'link_record'
        self.linked_table_id = linked_table_id
        self.link_id = link_id
        self.match_conditions = match_conditions
        self.linked_row_ids = []
        self._init_linked_row_ids()

    def parse_column_value(self, column, value):
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

    def _format_filter_groups(self):
        filters = []
        for match_condition in self.match_conditions:
            column_key = match_condition.get("column_key")
            column = self.get_column(self.auto_rule.table_id, column_key) or {}
            row_value = self.data['converted_row'].get(column.get('name'))
            if not row_value:
                return []
            other_column_key = match_condition.get("other_column_key")
            other_column = self.get_column(self.linked_table_id, other_column_key) or {}
            parsed_row_value = self.parse_column_value(other_column, row_value)
            filter_item = {
                "column_key": other_column_key,
                "filter_predicate": self.COLUMN_FILTER_PREDICATE_MAPPING.get(other_column.get('type', ''), 'is'),
                "filter_term": parsed_row_value,
                "filter_term_modifier":"exact_date"
            }
            filters.append(filter_item)
        return filters and [{"filters": filters, "filter_conjunction": "And"}] or []

    def get_table_name(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        for table in tables:
            if table.get('_id') == table_id:
                return table.get('name')

    def get_column(self, table_id, column_key):
        for col in self.get_columns(table_id):
            if col.get('key') == column_key:
                return col
        return None

    def get_columns(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                return table.get('columns', [])
        return []

    def _get_linked_table_rows(self):
        filter_groups = self._format_filter_groups()
        if not filter_groups:
            return []
        json_data = {
            'table_id': self.linked_table_id,
            'filter_conditions': {
                'filter_groups': filter_groups,
                'group_conjunction': 'And'
            },
            'limit': 500
        }
        try:
            response_data = self.auto_rule.dtable_server_api.internal_filter_rows(json_data)
            rows_data = response_data.get('rows')
            logger.debug('Number of linking dtable rows by auto-rules: %s, dtable_uuid: %s, details: %s' % (
                rows_data and len(rows_data) or 0,
                self.auto_rule.dtable_uuid,
                json.dumps(json_data)
            ))
            return rows_data or []
        except Exception as e:
            logger.error('link dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return []

    def _init_linked_row_ids(self):
        linked_rows_data = self._get_linked_table_rows()
        self.linked_row_ids = linked_rows_data and [row.get('_id') for row in linked_rows_data] or []

    def _can_do_action(self):
        table_columns = self.get_columns(self.auto_rule.table_id)
        link_col_name = ''
        for col in table_columns:
            if col.get('type') == 'link' and col.get('data', {}).get('link_id') == self.link_id:
                link_col_name = col.get('name')
        if link_col_name:
            linked_rows = self.data.get('converted_row', {}).get(link_col_name, {})
            table_linked_rows = {row.get('row_id'): True for row in linked_rows}
            if len(self.linked_row_ids) == len(table_linked_rows):
                for row_id in self.linked_row_ids:
                    if not table_linked_rows.get(row_id):
                        return True
                return False
        return True

    def do_action(self):
        if not self._can_do_action():
            return

        try:
            self.auto_rule.dtable_server_api.update_link(self.link_id, self.auto_rule.table_id, self.linked_table_id, self.data['row']['_id'], self.linked_row_ids)
        except Exception as e:
            logger.error('link dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()


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

    def __init__(self, auto_rule, data, row, dst_table_id):
        """
        auto_rule: instance of AutomationRule
        data: data is event data from redis
        row: {'col_1_name: ', value1, 'col_2_name': value2...}
        dst_table_id: id of table that record to be added
        """
        super().__init__(auto_rule, data)
        self.action_type = 'add_record_to_other_table'
        self.row = row
        self.col_name_dict = {}
        self.dst_table_id = dst_table_id
        self.row_data = {
            'row': {},
            'table_name': self.get_table_name(dst_table_id)
        }
        self._init_append_rows()

    def get_table_name(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        for table in tables:
            if table.get('_id') == table_id:
                return table.get('name')

    def get_columns(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                return table.get('columns', [])
        return []

    def _fill_msg_blanks(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, text, blanks, col_name_dict, row, db_session, dtable_metadata)

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def _init_append_rows(self):
        src_row = self.data['converted_row']
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}

        for row_id in self.row:
            cell_value = self.row.get(row_id)
            # cell_value may be dict if the column type is date
            if not isinstance(cell_value, str):
                continue
            blanks = set(re.findall(r'\{([^{]*?)\}', cell_value))
            self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
            self.row[row_id] = self._fill_msg_blanks(src_row, cell_value, self.column_blanks)

        dst_columns = self.get_columns(self.dst_table_id)

        filtered_updates = {}
        for col in dst_columns:
            if 'key' in col and col.get('type') in self.VALID_COLUMN_TYPES:
                col_name = col.get('name')
                col_type = col.get('type')
                col_key = col.get('key')
                if col_key in self.row.keys():
                    if col_type == ColumnTypes.DATE:
                        time_format = col.get('data', {}).get('format', '')
                        format_length = len(time_format.split(" "))
                        try:
                            time_dict = self.row.get(col_key)
                            if not time_dict:
                                continue
                            set_type = time_dict.get('set_type')
                            if set_type == 'specific_value':
                                time_value = time_dict.get('value')
                                filtered_updates[col_name] = time_value
                            elif set_type == 'relative_date':
                                offset = time_dict.get('offset')
                                filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                        except Exception as e:
                            logger.error(e)
                            filtered_updates[col_name] = self.row.get(col_key)
                    else:
                        filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))

        self.row_data['row'] = filtered_updates

    def _can_do_action(self):
        if not self.row_data.get('row'):
            return False

        return True

    def do_action(self):
        if not self._can_do_action():
            return

        try:
            self.auto_rule.dtable_server_api.append_row(self.get_table_name(self.dst_table_id), self.row_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()


class RuleInvalidException(Exception):
    """
    Exception which indicates rule need to be set is_valid=Fasle
    """
    pass


class AutomationRule:

    def __init__(self, data, db_session, raw_trigger, raw_actions, options, per_minute_trigger_limit=None):
        self.rule_id = options.get('rule_id', None)
        self.rule_name = ''
        self.run_condition = options.get('run_condition', None)
        self.dtable_uuid = options.get('dtable_uuid', None)
        self.trigger = None
        self.action_infos = []
        self.last_trigger_time = options.get('last_trigger_time', None)
        self.trigger_count = options.get('trigger_count', None)
        self.org_id = options.get('org_id', None)
        self.creator = options.get('creator', None)
        self.data = data
        self.db_session = db_session

        self.dtable_server_api = DTableServerAPI('Automation Rule', str(UUID(self.dtable_uuid)), get_inner_dtable_server_url())

        self.table_id = None
        self.view_id = None

        self._table_info = None
        self._view_info = None
        self._dtable_metadata = None
        self._access_token = None
        self._view_columns = None
        self.can_run_python = None
        self.scripts_running_limit = None

        self.cache_key = 'AUTOMATION_RULE:%s' % self.rule_id
        self.task_run_seccess = True

        self.done_actions = False
        self._load_trigger_and_actions(raw_trigger, raw_actions)

        self.current_valid = True

        self.per_minute_trigger_limit = per_minute_trigger_limit or 10

    def _load_trigger_and_actions(self, raw_trigger, raw_actions):
        self.trigger = json.loads(raw_trigger)

        self.table_id = self.trigger.get('table_id')
        if self.run_condition == PER_UPDATE:
            self._table_name = self.data.get('table_name', '')
        self.view_id = self.trigger.get('view_id')

        self.rule_name = self.trigger.get('rule_name', '')
        self.action_infos = json.loads(raw_actions)

    @property
    def access_token(self):

        if not self._access_token:
            self._access_token = jwt.encode(
                payload={
                    'exp': int(time.time()) + 300,
                    'dtable_uuid': uuid_str_to_36_chars(self.dtable_uuid),
                    'username': 'Automation Rule',
                    'permission': 'rw',
                },
                key=DTABLE_PRIVATE_KEY
            )
        return self._access_token

    @property
    def headers(self):
        return self.dtable_server_api.headers

    @property
    def dtable_metadata(self):
        if not self._dtable_metadata:
            try:
                self._dtable_metadata = self.dtable_server_api.get_metadata()
            except:
                raise RuleInvalidException('request metadata error')
            if not self._dtable_metadata:
                raise RuleInvalidException('request metadata parse error')
        return self._dtable_metadata

    @property
    def view_columns(self):
        """
        columns of the view defined in trigger
        """
        if not self._view_columns:
            table_name = self.table_info['name']
            view_name = self.view_info['name']
            try:
                self._view_columns = self.dtable_server_api.list_columns(table_name, view_name=view_name)
            except:
                raise RuleInvalidException('request view columns error')
            if not self._view_columns:
                raise RuleInvalidException('request view columns parse error')
        return self._view_columns

    @property
    def table_info(self):
        """
        name of table defined in rule
        """
        if not self._table_info:
            dtable_metadata = self.dtable_metadata
            tables = dtable_metadata.get('tables', [])
            for table in tables:
                if table.get('_id') == self.table_id:
                    self._table_info = table
                    break
            if not self._table_info:
                raise RuleInvalidException('table not found')
        return self._table_info

    @property
    def view_info(self):
        table_info = self.table_info
        if not self.view_id:
            self._view_info = table_info['views'][0]
            return self._view_info
        for view in table_info['views']:
            if view['_id'] == self.view_id:
                self._view_info = view
                break
        if not self._view_info:
            raise RuleInvalidException('view not found')
        return self._view_info


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

    def can_do_actions(self):
        if self.trigger.get('condition') not in (CONDITION_FILTERS_SATISFY, CONDITION_PERIODICALLY, CONDITION_ROWS_ADDED, CONDITION_PERIODICALLY_BY_CONDITION):
            return False

        if self.trigger.get('condition') == CONDITION_ROWS_ADDED:
            if self.data.get('op_type') not in ['insert_row', 'append_rows']:
                return False

        if self.trigger.get('condition') in [CONDITION_FILTERS_SATISFY, CONDITION_ROWS_MODIFIED]:
            if self.data.get('op_type') not in ['modify_row', 'modify_rows']:
                return False

        if self.run_condition == PER_UPDATE:
            # automation rule triggered by human or code, perhaps triggered quite quickly
            if self.per_minute_trigger_limit <= 0:
                return True
            trigger_times = redis_cache.get(self.cache_key)
            if not trigger_times:
                return True
            trigger_times = trigger_times.split(',')
            if len(trigger_times) >= self.per_minute_trigger_limit and time.time() - int(trigger_times[0]) < 60:
                return False
            return True

        elif self.run_condition in (PER_DAY, PER_WEEK, PER_MONTH):
            cur_datetime = datetime.now()
            cur_hour = cur_datetime.hour
            cur_week_day = cur_datetime.isoweekday()
            cur_month_day = cur_datetime.day
            if self.run_condition == PER_DAY:
                trigger_hour = self.trigger.get('notify_hour', 12)
                if cur_hour != trigger_hour:
                    return False
            elif self.run_condition == PER_WEEK:
                trigger_hour = self.trigger.get('notify_week_hour', 12)
                trigger_day = self.trigger.get('notify_week_day', 7)
                if cur_hour != trigger_hour or cur_week_day != trigger_day:
                    return False
            else:
                trigger_hour = self.trigger.get('notify_month_hour', 12)
                trigger_day = self.trigger.get('notify_month_day', 1)
                if cur_hour != trigger_hour or cur_month_day != trigger_day:
                    return False
            return True

        return False


    def do_actions(self, with_test=False):
        if (not self.can_do_actions()) and (not with_test):
            return

        for action_info in self.action_infos:
            logger.debug('start action: %s type: %s', action_info.get('_id'), action_info['type'])
            try:
                if action_info.get('type') == 'update_record':
                    updates = action_info.get('updates')
                    UpdateAction(self, self.data, updates).do_action()

                if action_info.get('type') == 'add_record':
                    row = action_info.get('row')
                    AddRowAction(self, row).do_action()

                elif action_info.get('type') == 'notify':
                    default_msg = action_info.get('default_msg', '')
                    users = action_info.get('users', [])
                    users_column_key = action_info.get('users_column_key', '')
                    NotifyAction(self, self.data, default_msg, users, users_column_key).do_action()

                elif action_info.get('type') == 'lock_record':
                    LockRowAction(self, self.data, self.trigger).do_action()

                elif action_info.get('type') == 'send_wechat':
                    account_id = int(action_info.get('account_id'))
                    default_msg = action_info.get('default_msg', '')
                    msg_type = action_info.get('msg_type', 'text')
                    SendWechatAction(self, self.data, default_msg, account_id, msg_type).do_action()

                elif action_info.get('type') == 'send_dingtalk':
                    account_id = int(action_info.get('account_id'))
                    default_msg = action_info.get('default_msg', '')
                    default_title = action_info.get('default_title', '')
                    msg_type = action_info.get('msg_type', 'text')
                    SendDingtalkAction(self, self.data, default_msg, account_id, msg_type, default_title).do_action()

                elif action_info.get('type') == 'send_email':
                    account_id = int(action_info.get('account_id'))
                    msg = action_info.get('default_msg', '')
                    subject = action_info.get('subject', '')
                    send_to_list = email2list(action_info.get('send_to', ''))
                    copy_to_list = email2list(action_info.get('copy_to', ''))

                    send_info = {
                        'message': msg,
                        'send_to': send_to_list,
                        'copy_to': copy_to_list,
                        'subject': subject
                    }
                    SendEmailAction(self, self.data, send_info, account_id).do_action()

                elif action_info.get('type') == 'run_python_script':
                    script_name = action_info.get('script_name')
                    workspace_id = action_info.get('workspace_id')
                    owner = action_info.get('owner')
                    org_id = action_info.get('org_id')
                    repo_id = action_info.get('repo_id')
                    RunPythonScriptAction(self, self.data, script_name, workspace_id, owner, org_id, repo_id).do_action()

                elif action_info.get('type') == 'link_records':
                    linked_table_id = action_info.get('linked_table_id')
                    link_id = action_info.get('link_id')
                    match_conditions = action_info.get('match_conditions')
                    if self.run_condition == PER_UPDATE:
                        LinkRecordsAction(self, self.data, linked_table_id, link_id, match_conditions).do_action()

                elif action_info.get('type') == 'add_record_to_other_table':
                    row = action_info.get('row')
                    dst_table_id = action_info.get('dst_table_id')
                    AddRecordToOtherTableAction(self, self.data, row, dst_table_id).do_action()

            except RuleInvalidException as e:
                logger.error('auto rule: %s, invalid error: %s', self.rule_id, e)
                self.task_run_seccess = False
                self.set_invalid()
                break
            except Exception as e:
                logger.exception(e)
                self.task_run_seccess = False
                logger.error('rule: %s, do actions error: %s', self.rule_id, e)

        if self.done_actions and not with_test:
            self.update_last_trigger_time()

        if not with_test:
            self.add_task_log()

    def set_done_actions(self, done=True):
        self.done_actions = done

    def add_task_log(self):
        if not self.org_id:
            return
        try:
            set_task_log_sql = """
                INSERT INTO auto_rules_task_log (trigger_time, success, rule_id, run_condition, dtable_uuid, org_id, owner) VALUES
                (:trigger_time, :success, :rule_id, :run_condition, :dtable_uuid, :org_id, :owner)
            """
            if self.run_condition in (PER_DAY, PER_WEEK, PER_MONTH, PER_UPDATE):
                self.db_session.execute(set_task_log_sql, {
                    'trigger_time': datetime.utcnow(),
                    'success': self.task_run_seccess,
                    'rule_id': self.rule_id,
                    'run_condition': self.run_condition,
                    'dtable_uuid': self.dtable_uuid,
                    'org_id': self.org_id,
                    'owner': self.creator,
                })
                self.db_session.commit()
        except Exception as e:
            logger.error('set rule task log: %s invalid error: %s', self.rule_id, e)

    def update_last_trigger_time(self):
        try:
            set_statistic_sql_user = '''
                INSERT INTO user_auto_rules_statistics (username, trigger_date, trigger_count, update_at) VALUES 
                (:username, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''

            set_statistic_sql_org = '''
                INSERT INTO org_auto_rules_statistics (org_id, trigger_date, trigger_count, update_at) VALUES
                (:org_id, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''
            set_last_trigger_time_sql = '''
                UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time, trigger_count=:trigger_count WHERE id=:rule_id;
            '''

            org_id = self.org_id
            if not org_id:
                sql = set_last_trigger_time_sql
            else:
                sql = "%s%s" % (set_last_trigger_time_sql, set_statistic_sql_user if self.org_id == -1 else set_statistic_sql_org)

            cur_date = datetime.now().date()
            cur_year, cur_month = cur_date.year, cur_date.month
            trigger_date = date(year=cur_year, month=cur_month, day=1)
            self.db_session.execute(sql, {
                'rule_id': self.rule_id,
                'trigger_time': datetime.utcnow(),
                'trigger_date': trigger_date,
                'trigger_count': self.trigger_count + 1,
                'username': self.creator,
                'org_id': self.org_id
            })
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)

        if self.run_condition == PER_UPDATE and self.per_minute_trigger_limit > 0:
            trigger_times = redis_cache.get(self.cache_key)
            if not trigger_times:
                redis_cache.set(self.cache_key, int(time.time()), timeout=MINUTE_TIMEOUT)
            else:
                trigger_times = trigger_times.split(',')
                trigger_times.append(str(int(time.time())))
                trigger_times = trigger_times[-self.per_minute_trigger_limit:]
                redis_cache.set(self.cache_key, ','.join([t for t in trigger_times]), timeout=MINUTE_TIMEOUT)

    def set_invalid(self):
        try:
            self.current_valid = False
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET is_valid=0 WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id})
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)
