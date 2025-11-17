import json
from datetime import date
from types import SimpleNamespace

from sqlalchemy import text

from dtable_events.ccnet.user import get_user_role
from dtable_events.notification_rules.notification_rules_utils import send_notification
from dtable_events.utils import get_dtable_admins

from dtable_events.app.config import CCNET_DB_NAME, DTABLE_WEB_SERVICE_URL
from dtable_events.utils.dtable_web_api import DTableWebAPI


class AutomationsStatsHelper:

    def __init__(self):
        self.dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
        self.roles = None

        self.ccnet_db_name = CCNET_DB_NAME

    def get_roles(self):
        if self.roles:
            return self.roles
        self.roles = self.dtable_web_api.internal_roles()
        return self.roles

    def get_user_quota(self, db_session, username):
        sql = "SELECT username, automation_limit_per_month FROM user_quota WHERE username=:username"
        row = db_session.execute(text(sql), {'username': username}).fetchone()
        if row and row.automation_limit_per_month and row.automation_limit_per_month != 0:
            return row.automation_limit_per_month
        user_role = get_user_role(db_session, username)
        return self.get_roles().get(user_role, {}).get('automation_limit_per_month', -1)

    def get_org_quota(self, db_session, org_id):
        sql = "SELECT org_id, automation_limit_per_month FROM organizations_org_quota WHERE org_id=:org_id"
        row = db_session.execute(text(sql), {'org_id': org_id}).fetchone()
        if row and row.automation_limit_per_month and row.automation_limit_per_month != 0:
            return row.automation_limit_per_month
        sql = "SELECT role FROM organizations_orgsettings WHERE org_id=:org_id"
        row = db_session.execute(text(sql), {'org_id': org_id}).fetchone()
        if not row:
            org_role = 'org_default'
        else:
            org_role = row.role
        return self.get_roles().get(org_role, {}).get('automation_limit_per_month', -1)

    def get_user_usage(self, db_session, username):
        """
        :return: row with (trigger_count, has_sent_warning, warning_limit)
        """
        sql = "SELECT trigger_count, has_sent_warning, warning_limit FROM user_auto_rules_statistics WHERE username=:username AND trigger_date=:trigger_date"
        row = db_session.execute(text(sql), {'username': username, 'trigger_date': date.today().replace(day=1)}).fetchone()
        if not row:
            return SimpleNamespace(**{'trigger_count': 0, 'has_sent_warning': 0, 'warning_limit': None})
        return row

    def get_org_usage(self, db_session, org_id):
        """
        :return: trigger_count -> int, has_sent_warning -> bool
        """
        sql = "SELECT trigger_count, has_sent_warning, warning_limit FROM org_auto_rules_statistics WHERE org_id=:org_id AND trigger_date=:trigger_date"
        row = db_session.execute(text(sql), {'org_id': org_id, 'trigger_date': date.today().replace(day=1)}).fetchone()
        if not row:
            return SimpleNamespace(**{'trigger_count': 0, 'has_sent_warning': 0, 'warning_limit': None})
        return row

    def is_exceed(self, db_session, owner, org_id):
        if org_id == -1:
            if '@seafile_group' in owner:
                return False
            quota = self.get_user_quota(db_session, owner)
            if quota < 0:
                return False
            usage = self.get_user_usage(db_session, owner).trigger_count
            return quota <= usage
        else:
            quota = self.get_org_quota(db_session, org_id)
            if quota < 0:
                return False
            usage = self.get_org_usage(db_session, org_id).trigger_count
            return quota <= usage

    def check_user_reach_warning(self, db_session, username):
        limit = self.get_user_quota(db_session, username)
        if limit < 0:
            return
        usage = self.get_user_usage(db_session, username)
        if (not usage.has_sent_warning and usage.trigger_count >= limit * 0.9) \
            or (usage.has_sent_warning and usage.trigger_count >= limit * 0.9 and usage.warning_limit != limit):
            self.dtable_web_api.internal_add_notification([username], 'automation_limit_reach_warning', {'limit': limit, 'usage': usage.trigger_count})
            sql = "UPDATE user_auto_rules_statistics SET has_sent_warning=1, warning_limit=:warning_limit WHERE username=:username AND trigger_date=:trigger_date"
            db_session.execute(text(sql), {'username': username, 'warning_limit': limit, 'trigger_date': date.today().replace(day=1)})
            db_session.commit()

    def check_org_reach_warning(self, db_session, org_id):
        limit = self.get_org_quota(db_session, org_id)
        if limit < 0:
            return
        usage = self.get_org_usage(db_session, org_id)
        if (not usage.has_sent_warning and usage.trigger_count >= limit * 0.9) \
            or (usage.has_sent_warning and usage.trigger_count >= limit * 0.9 and usage.warning_limit != limit):
            admins = []
            sql = "SELECT email FROM %s.OrgUser WHERE org_id=:org_id AND is_staff=1" % self.ccnet_db_name
            for row in db_session.execute(text(sql), {'org_id': org_id}):
                admins.append(row.email)
            self.dtable_web_api.internal_add_notification(admins, 'automation_limit_reach_warning', {'limit': limit, 'usage': usage.trigger_count})
            sql = "UPDATE org_auto_rules_statistics SET has_sent_warning=1, warning_limit=:warning_limit WHERE org_id=:org_id AND trigger_date=:trigger_date"
            db_session.execute(text(sql), {'org_id': org_id, 'warning_limit': limit, 'trigger_date': date.today().replace(day=1)})
            db_session.commit()

    def update_stats(self, db_session, auto_rule_result):
        # update rule, rule_log, stats
        statistic_sql_user = '''
            INSERT INTO user_auto_rules_statistics (username, trigger_date, trigger_count, update_at) VALUES 
            (:username, :trigger_date, 1, :trigger_time)
            ON DUPLICATE KEY UPDATE
            trigger_count=trigger_count+1,
            update_at=:trigger_time
        '''
        statistic_sql_org = '''
            INSERT INTO org_auto_rules_statistics (org_id, trigger_date, trigger_count, update_at) VALUES
            (:org_id, :trigger_date, 1, :trigger_time)
            ON DUPLICATE KEY UPDATE
            trigger_count=trigger_count+1,
            update_at=:trigger_time
        '''
        update_rule_sql = '''
            UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time, is_valid=:is_valid, trigger_count=trigger_count+1 WHERE id=:rule_id;
        '''
        insert_rule_log = '''
            INSERT INTO auto_rules_task_log (trigger_time, success, rule_id, run_condition, dtable_uuid, org_id, owner, warnings) VALUES
            (:trigger_time, :success, :rule_id, :run_condition, :dtable_uuid, :org_id, :owner, :warnings)
        '''
        org_id = auto_rule_result.get('org_id')
        owner = auto_rule_result.get('owner')
        sqls = [update_rule_sql, insert_rule_log]
        if org_id:
            if org_id == -1:
                if '@seafile_group' not in owner:
                    sqls.append(statistic_sql_user)
            else:
                sqls.append(statistic_sql_org)
        params = {
            'rule_id': auto_rule_result.get('rule_id'),
            'username': owner,
            'dtable_uuid': auto_rule_result.get('dtable_uuid'),
            'org_id': org_id,
            'owner': auto_rule_result.get('owner'),
            'trigger_time': auto_rule_result.get('trigger_time'),
            'trigger_date': auto_rule_result.get('trigger_date'),
            'is_valid': auto_rule_result.get('is_valid'),
            'success': 1 if auto_rule_result.get('success') else 0,
            'run_condition': auto_rule_result.get('run_condition'),
            'warnings': json.dumps(auto_rule_result.get('warnings')) if json.dumps(auto_rule_result.get('warnings')) else None
        }
        for sql in sqls:
            db_session.execute(text(sql), params)
        db_session.commit()

        # send reach warning
        if org_id == -1 and owner and '@seafile_group' not in owner:
            self.check_user_reach_warning(db_session, owner)
        elif org_id != -1:
            self.check_org_reach_warning(db_session, org_id)

        # send invalid warning
        if auto_rule_result.get('is_valid') == False:
            admins = get_dtable_admins(auto_rule_result.get('dtable_uuid'), db_session)
            invalid_type = auto_rule_result.get('invalid_type') or ''
            send_notification(auto_rule_result.get('dtable_uuid'), [{
                'to_user': user,
                'msg_type': 'auto_rule_invalid',
                'detail': {
                    'author': 'Automation Rule',
                    'rule_id': auto_rule_result.get('rule_id'),
                    'rule_name': auto_rule_result.get('rule_name'),
                    'invalid_type': invalid_type
                }
            } for user in admins])
