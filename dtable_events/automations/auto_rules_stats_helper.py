from datetime import date
from types import SimpleNamespace

from sqlalchemy import text

from seaserv import ccnet_api

from dtable_events.app.config import CCNET_DB_NAME, DTABLE_WEB_SERVICE_URL
from dtable_events.app.log import auto_rule_logger
from dtable_events.utils.dtable_web_api import DTableWebAPI


class AutoRulesStatsHelper:

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
        sql = "SELECT username, automation_rules_limit_per_month FROM user_quota WHERE username=:username"
        row = db_session.execute(text(sql), {'username': username}).fetchone()
        if row and row.automation_rules_limit_per_month and row.automation_rules_limit_per_month != 0:
            return row.automation_rules_limit_per_month
        user = ccnet_api.get_emailuser(username)
        user_role = user.role
        return self.get_roles().get(user_role, {}).get('automation_rules_limit_per_month', -1)

    def get_org_quota(self, db_session, org_id):
        sql = "SELECT org_id, automation_rules_limit_per_month FROM organizations_org_quota WHERE org_id=:org_id"
        row = db_session.execute(text(sql), {'org_id': org_id}).fetchone()
        if row and row.automation_rules_limit_per_month and row.automation_rules_limit_per_month != 0:
            return row.automation_rules_limit_per_month
        sql = "SELECT role FROM organizations_orgsettings WHERE org_id=:org_id"
        row = db_session.execute(text(sql), {'org_id': org_id}).fetchone()
        if not row:
            org_role = 'org_default'  # check from dtable-web/seahub/role_permissions/settings DEFAULT_ENABLED_ROLE_PERMISSIONS[ORG_DEFAULT]
        else:
            org_role = row.role
        return self.get_roles().get(org_role, {}).get('automation_rules_limit_per_month', -1)

    def get_user_usage(self, db_session, username):
        """
        :return: row with (trigger_count, has_sent_warning, warning_limit)
        """
        sql = "SELECT trigger_count, has_sent_warning, warning_limit FROM user_auto_rules_statistics_per_month WHERE username=:username AND month=:month"
        row = db_session.execute(text(sql), {'username': username, 'month': date.today().replace(day=1)}).fetchone()
        if not row:
            return SimpleNamespace(**{'trigger_count': 0, 'has_sent_warning': 0, 'warning_limit': None})
        return row

    def get_org_usage(self, db_session, org_id):
        """
        :return: trigger_count -> int, has_sent_warning -> bool
        """
        sql = "SELECT trigger_count, has_sent_warning, warning_limit FROM org_auto_rules_statistics_per_month WHERE org_id=:org_id AND month=:month"
        row = db_session.execute(text(sql), {'org_id': org_id, 'month': date.today().replace(day=1)}).fetchone()
        if not row:
            return SimpleNamespace(**{'trigger_count': 0, 'has_sent_warning': 0, 'warning_limit': None})
        return row

    def update_user(self, db_session, username):
        limit = self.get_user_quota(db_session, username)
        if limit < 0:
            return
        usage = self.get_user_usage(db_session, username)
        if (not usage.has_sent_warning and usage.trigger_count >= limit * 0.9) \
            or (usage.has_sent_warning and usage.trigger_count >= limit * 0.9 and usage.warning_limit != limit):
            self.dtable_web_api.internal_add_notification([username], 'autorule_limit_reached_warning', {'limit': limit, 'usage': usage.trigger_count})
            sql = "UPDATE user_auto_rules_statistics_per_month SET has_sent_warning=1, warning_limit=:warning_limit WHERE username=:username AND month=:month"
            db_session.execute(text(sql), {'username': username, 'warning_limit': limit, 'month': date.today().replace(day=1)})
            db_session.commit()

    def update_org(self, db_session, org_id):
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
            self.dtable_web_api.internal_add_notification(admins, 'autorule_limit_reached_warning', {'limit': limit, 'usage': usage.trigger_count})
            sql = "UPDATE org_auto_rules_statistics_per_month SET has_sent_warning=1, warning_limit=:warning_limit WHERE org_id=:org_id AND month=:month"
            db_session.execute(text(sql), {'org_id': org_id, 'warning_limit': limit, 'month': date.today()})
            db_session.commit()

    def update_stats(self, db_session, auto_rule_info):
        owner = auto_rule_info.get('owner')
        org_id = auto_rule_info.get('org_id')
        try:
            if org_id == -1 and owner:
                self.update_user(db_session, owner)
            elif org_id != -1:
                self.update_org(db_session, org_id)
        except Exception as e:
            auto_rule_logger.exception('update stats info: %s error: %s', auto_rule_info, e)


auto_rules_stats_helper = AutoRulesStatsHelper()
