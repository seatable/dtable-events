import json
import logging
import re
import time
import os
from datetime import datetime, date, timedelta

import jwt
import requests

from dtable_events.activities.notification_rules_utils import _fill_msg_blanks as fill_msg_blanks, \
    send_notification
from dtable_events.utils import utc_to_tz
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
try:
    # from seahub.settings import DTABLE_PRIVATE_KEY, DTABLE_SERVER_URL
    import seahub.settings as seahub_settings
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    TIME_ZONE = getattr(seahub_settings, 'TIME_ZONE', 'UTC')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'

MESSAGE_TYPE_AUTOMATION_RULE = 'automation_rule'


class BaseAction:

    def __init__(self, auto_rule, data):
        self.auto_rule = auto_rule
        self.action_type = 'base'
        self.data = data

    def do_action(self):
        pass


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
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
    ]

    def __init__(self, auto_rule, data, updates):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis, else is rows from request
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, data)
        self.action_type = 'update'
        self.updates = updates
        self.update_data = {
            'updates': [],
            'table_name': self.auto_rule.get_table_name_by_id(auto_rule.table_id)
        }
        self._init_updates()

    def _init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        valid_view_column_names = [col.get('name') for col in self.auto_rule.view_columns if 'name' in col and col.get('type') in self.VALID_COLUMN_TYPES]
        filtered_updates = {key: value for key, value in self.updates.items() if key in valid_view_column_names}

        if self.auto_rule.run_condition == PER_UPDATE:
            row_id = self.data.get('row_id')
            self.update_data['updates'].append({
                'row_id': row_id,
                'row': filtered_updates
            })
        elif self.auto_rule.run_condition in (PER_DAY, PER_WEEK):
            for row in self.data:
                row_id = row.get('_id')
                self.update_data['updates'].append({
                    'row_id': row_id,
                    'row': filtered_updates
                })

    def _can_do_action(self):
        if not self.update_data.get('updates'):
            return False
        if self.auto_rule.run_condition == PER_UPDATE:
            # For preventing loop updating, if the cell of the columns in action's updates is updated, forbidden action!!!
            for cell in self.data.get('row_data', []):
                if cell.get('column_name') in self.updates:
                    return False
        return True

    def do_action(self):
        if not self._can_do_action():
            return
        batch_update_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.auto_rule.dtable_uuid + '/batch-update-rows/'
        try:
            response = requests.put(batch_update_url, headers=self.auto_rule.headers, json=self.update_data)
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        if response.status_code != 200:
            logger.error('update dtable: %s error response status code: %s', self.auto_rule.dtable_uuid, response.status_code)
        else:
            self.auto_rule.set_done_actions()


class NotifyAction(BaseAction):

    def __init__(self, auto_rule, data, msg, users):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis, else is rows from request
        msg: message set in action
        users: who will receive notification(s)
        """
        super().__init__(auto_rule, data)
        self.action_type = 'notify'
        self.msg = msg
        self.users = users

        self.column_blanks = []
        self.col_name_dict = {}
        self._init_msg(msg)

    def _init_msg(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.view_columns}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def _fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        dtable_uuid, db_session, dtable_metadata = self.auto_rule.dtable_uuid, self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks(dtable_uuid, msg, column_blanks, col_name_dict, row, db_session, dtable_metadata)

    def _can_do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            last_trigger_time = self.auto_rule.last_trigger_time
            if last_trigger_time and (datetime.utcnow() - last_trigger_time).seconds < 300:
                return False
        return True

    def per_update_notify(self):
        dtable_uuid, row_id = self.auto_rule.dtable_uuid, self.data.get('row_id')
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        msg = self.msg
        if self.column_blanks:
            row_url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/{dtable_uuid}/rows/{row_id}/'.format(dtable_uuid=dtable_uuid, row_id=row_id)
            params = {'table_id': table_id, 'convert_link_id': True}
            try:
                response = requests.get(row_url, headers=self.auto_rule.headers, params=params)
                row = response.json()
            except Exception as e:
                logger.error('request dtable: %s row: %s error: %s', dtable_uuid, row_id, e)
                return
            if response.status_code != 200:
                logger.error('request dtable: %s row: %s error status code: %s', dtable_uuid, row_id, response.status_code)
                return

            msg = self._fill_msg_blanks(row)

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_ROWS_MODIFIED,
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': msg,
            'row_id_list': [row_id],
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

    def cron_notify(self):
        dtable_uuid = self.auto_rule.dtable_uuid
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        rows = self.data
        for row in rows:
            msg = self.msg
            if self.column_blanks:
                msg = self._fill_msg_blanks(row)

            detail = {
                'table_id': table_id,
                'view_id': view_id,
                'condition': CONDITION_ROWS_MODIFIED,
                'rule_id': self.auto_rule.rule_id,
                'rule_name': self.auto_rule.rule_name,
                'msg': msg,
                'row_id_list': [row.get('_id')],
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
        if not self._can_do_action():
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
            self.auto_rule.set_done_actions()
        elif self.auto_rule.run_condition in [PER_DAY, PER_WEEK]:
            self.cron_notify()
            self.auto_rule.set_done_actions()


class RuleInvalidException(Exception):
    """
    Exception which indicates rule need to be set is_valid=Fasle
    """
    pass


class AutomationRule:

    def __init__(self, rule_id, run_condition, dtable_uuid, raw_trigger, raw_actions, last_trigger_time, data, db_session):
        self.rule_id = rule_id
        self.rule_name = ''
        self.run_condition = run_condition
        self.dtable_uuid = dtable_uuid
        self.trigger = None
        self.action_infos = []
        self.last_trigger_time = last_trigger_time
        self.data = data
        self.db_session = db_session

        self.table_id = None
        self.view_id = None

        self._dtable_metadata = None
        self._access_token = None
        self._table_columns = None
        self._view_columns = None

        self.done_actions = False

        self._load_trigger_and_actions(raw_trigger, raw_actions)

    def _load_trigger_and_actions(self, raw_trigger, raw_actions):
        self.trigger = json.loads(raw_trigger)

        self.table_id = self.trigger.get('table_id')
        self.view_id = self.trigger.get('view_id')

        self.rule_name = self.trigger.get('rule_name', '')
        self.action_infos = json.loads(raw_actions)

    @property
    def access_token(self):
        if not self._access_token:
            self._access_token = jwt.encode(
                payload={
                    'exp': int(time.time()) + 300,
                    'dtable_uuid': self.dtable_uuid,
                    'username': 'system',
                    'permission': 'rw',
                },
                key=DTABLE_PRIVATE_KEY
            )

        return self._access_token

    @property
    def headers(self):
        return {'Authorization': 'Token ' + self.access_token.decode()}

    @property
    def dtable_metadata(self):
        if not self._dtable_metadata:
            url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/metadata/'
            response = requests.get(url, headers=self.headers)
            if response.status_code == 404:
                raise RuleInvalidException('request metadata 404')
            self._dtable_metadata = response.json().get('metadata')
        return self._dtable_metadata

    @property
    def table_columns(self):
        if not self._table_columns:
            table_id = self.table_id
            url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'
            response = requests.get(url, params={'table_id': table_id}, headers=self.headers)
            if response.status_code == 404:
                raise RuleInvalidException('request table columns 404')
            self._table_columns = response.json().get('columns')
        return self._table_columns

    @property
    def view_columns(self):
        """
        columns of view defined in trigger
        """
        if not self._view_columns:
            table_id, view_id = self.table_id, self.view_id
            url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/columns/'
            response = requests.get(url, params={'table_id': table_id, 'view_id': view_id}, headers=self.headers)
            if response.status_code == 404:
                raise RuleInvalidException('request view columns 404')
            self._view_columns = response.json().get('columns')
        return self._view_columns

    def list_rows_near_deadline(self):
        url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/rows/'
        query_params = {
            'table_id': self.table_id,
            'view_id': self.view_id,
            'convert_link_id': True
        }
        try:
            response = requests.get(url, params=query_params, headers=self.headers)
        except Exception as e:
            logger.error('rule: %s request rows error: %s', self.rule_id, e)
            return []
        if response.status_code == 404:
            raise RuleInvalidException('request rows 404')
        if response.status_code != 200:
            logger.error('rule: %s request rows error status code: %s', self.rule_id, response.status_code)
            return []
        rows = response.json().get('rows', [])
        rows_near_deadline = []
        date_column_name = self.trigger.get('date_column_name', '')
        trigger_days = self.trigger.get('trigger_days', 0)
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
            now_plus_alarm_date = date.today() + timedelta(days=int(trigger_days))
            if date.today() <= deadline_date <= now_plus_alarm_date:
                rows_near_deadline.append(row)
        return rows_near_deadline

    def is_view_in_table(self, view_id, table_id):
        dtable_metadata = self.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') != table_id:
                continue
            for view in table.get('views', []):
                if view.get('_id') == view_id:
                    return True
        return False

    def is_row_in_view(self, row_id):
        url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/tables/' + self.table_id + '/is-row-in-view/'
        headers = {'Authorization': 'Token ' + self.access_token.decode('utf-8')}
        params = {
            'row_id': row_id,
            'view_id': self.view_id,
        }
        res = requests.get(url, headers=headers, params=params)

        if res.status_code == 404:
            # perhaps 404 is reason for row_id, we only deal with 'view not found','table not found' and 'dtable not found'
            if not self.is_view_in_table(self.view_id, self.table_id):
                raise RuleInvalidException('view not in table')
        if res.status_code != 200:
            logger.error('request is_row_in_view error status code: %s', res.status_code)
            return False
        return json.loads(res.content).get('is_row_in_view')

    def is_row_satisfy_filters(self, row_id):
        filters = self.trigger.get('filters', [])
        filter_conjuntion = self.trigger.get('filter_conjunction', 'And')

        url = DTABLE_SERVER_URL.rstrip('/') + '/api/v1/dtables/' + self.dtable_uuid + '/tables/' + self.table_id + '/is-row-satisfy-filters/'
        headers = {'Authorization': 'Token ' + self.access_token.decode('utf-8')}
        data = {
            'row_id': row_id,
            'filters': filters,
            'filter_conjunction': filter_conjuntion
        }

        res = requests.get(url, headers=headers, json=data)

        if res.status_code == 404:
            raise RuleInvalidException('verify filters 404 error')
        if res.status_code != 200:
            logger.error(res.text)
            return False

        return json.loads(res.content).get('is_row_satisfy_filters')

    def get_table_name_by_id(self, table_id):
        for table in self.dtable_metadata.get('tables'):
            if table.get('_id') == table_id:
                 return table.get('name')
        return None

    def can_do_actions(self):
        """
        judge auto rule whether can do actions
        """
        # if actions are notify type, check last_trigger_time firstly
        is_all_notify_actions = True
        for action_info in self.action_infos:
            if action_info.get('type') != 'notify':
                is_all_notify_actions = False
                break
        if is_all_notify_actions:
            if self.last_trigger_time and (datetime.utcnow() - self.last_trigger_time).seconds < 300:
                return False

        if self.trigger.get('condition') in (CONDITION_FILTERS_SATISFY, CONDITION_ROWS_MODIFIED):
            # row in view
            row_id, table_id = self.data.get('row_id'), self.data.get('table_id')
            column_keys = [cell.get('column_key') for cell in self.data.get('row_data', []) if 'column_key' in cell]
            if not row_id or not table_id:
                return False
            if table_id != self.table_id:
                return False

            if not self.is_row_in_view(row_id):
                return False

            # watch all columns and target column keys
            target_column_keys = self.trigger.get('column_keys', [])
            watch_all_columns = self.trigger.get('watch_all_columns', True)
            if not watch_all_columns:
                has_msg_key_in_target_keys = False
                for msg_key in column_keys:
                    if msg_key in target_column_keys:
                        has_msg_key_in_target_keys = True
                        break
                if not has_msg_key_in_target_keys:
                    return False

            if self.trigger.get('condition') == CONDITION_FILTERS_SATISFY:
                if not self.is_row_satisfy_filters(row_id):
                    return False

            return True

        elif self.trigger.get('condition') in (CONDITION_NEAR_DEADLINE,):
            trigger_hour = self.trigger.get('trigger_hour', 12)
            cur_hour = int(utc_to_tz(datetime.utcnow(), TIME_ZONE).strftime('%H'))
            if cur_hour != trigger_hour:
                return False
            rows = self.list_rows_near_deadline()
            if not rows:
                return False
            self.data = rows
            return True

        return False

    def do_actions(self):
        try:
            if not self.can_do_actions():
                return
            for action_info in self.action_infos:
                if action_info.get('type') == 'update':
                    updates = action_info.get('updates')
                    UpdateAction(self, self.data, updates).do_action()

                elif action_info.get('type') == 'notify':
                    default_msg = action_info.get('default_msg', '')
                    users = action_info.get('users', [])
                    NotifyAction(self, self.data, default_msg, users).do_action()
        except RuleInvalidException as e:
            logger.error('auto rule: %s, invalid error: %s', self.rule_id, e)
            self.set_invalid()
        except Exception as e:
            logger.exception(e)
            logger.error('rule: %s, do actions error: %s', self.rule_id, e)
        else:
            if self.done_actions:
                self.update_last_trigger_time()

    def set_done_actions(self, done=True):
        self.done_actions = done

    def update_last_trigger_time(self):
        try:
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id, 'trigger_time': datetime.utcnow()})
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)

    def set_invalid(self):
        try:
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET is_valid=0 WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id})
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)
