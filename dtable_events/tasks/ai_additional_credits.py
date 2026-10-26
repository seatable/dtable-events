import json
import logging

from sqlalchemy import text

from dtable_events.app.config import INNER_DTABLE_WEB_SERVICE_URL, ORG_MEMBER_QUOTA_DEFAULT
from dtable_events.app.event_redis import redis_cache
from dtable_events.utils.dtable_web_api import DTableWebAPI

logger = logging.getLogger(__name__)


class AIAdditionalCreditsManager(object):

    def __init__(self, db_session_class):
        self._db_session_class = db_session_class
        self.dtable_web_api = DTableWebAPI(INNER_DTABLE_WEB_SERVICE_URL)
        self.roles_cache_key = 'DTABLE_WEB_ROLES'
        self.roles_cache_timeout = 600

    def get_roles(self):
        roles_json = redis_cache.get(self.roles_cache_key)
        if not roles_json:
            roles = self.dtable_web_api.internal_roles()
            redis_cache.set(self.roles_cache_key, json.dumps(roles), timeout=self.roles_cache_timeout)
            return roles
        return json.loads(roles_json)

    def get_org_role(self, db_session, org_id):
        sql = "SELECT role FROM organizations_orgsettings WHERE org_id=:org_id"
        row = db_session.execute(text(sql), {'org_id': org_id}).fetchone()
        if not row:
            org_role = 'org_default'
        else:
            org_role = row.role
        return org_role

    def get_monthly_credit(self, session, org_id):
        org_role = self.get_org_role(session, org_id)
        ai_credit_per_user = self.get_roles().get(org_role, {}).get('ai_credit_per_user', -1)
        if ai_credit_per_user < 0:
            return -1

        row = session.execute(text("SELECT quota FROM organizations_orgmemberquota WHERE org_id=:org_id"),
                              {'org_id': org_id}).fetchone()
        member_quota = row.quota if row else ORG_MEMBER_QUOTA_DEFAULT
        return ai_credit_per_user * member_quota

    def get_month_cost(self, session, org_id, month):
        row = session.execute(text(
            "SELECT COALESCE(SUM(cost), 0) AS total_cost FROM stats_ai_by_team WHERE org_id=:org_id AND month=:month"
        ), {'org_id': org_id, 'month': month}).fetchone()
        return float(row.total_cost or 0)

    def deduct_overflow_credits(self, org_id, cost_delta, month):
        """Deduct additional credits for the part of cost_delta that exceeds the monthly credit.

        Must be called after cost_delta has been written to stats_ai_by_team.
        """
        if cost_delta <= 0:
            return None

        session = self._db_session_class()
        try:
            monthly_credit = self.get_monthly_credit(session, org_id)
            if monthly_credit < 0:
                return None

            used = self.get_month_cost(session, org_id, month)
            current_overflow = max(0.0, used - monthly_credit)
            previous_overflow = max(0.0, used - cost_delta - monthly_credit)
            to_deduct = current_overflow - previous_overflow
            if to_deduct <= 0:
                return None

            row = session.execute(text("SELECT credits FROM ai_additional_credits WHERE org_id=:org_id FOR UPDATE"),
                                  {'org_id': org_id}).fetchone()
            credits = float(row.credits or 0) if row else 0.0
            deducted = min(credits, to_deduct)
            remaining = credits - deducted
            if row:
                session.execute(text("UPDATE ai_additional_credits SET credits=:credits, updated_at=NOW() WHERE org_id=:org_id"),
                                {'credits': remaining, 'org_id': org_id})
            else:
                session.execute(text(
                    "INSERT INTO ai_additional_credits (org_id, credits, updated_at) "
                    "VALUES (:org_id, 0, NOW()) "
                    "ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)"
                ), {'org_id': org_id})
            session.commit()
            return {
                'monthly_credit': monthly_credit,
                'overflow': to_deduct,
                'deducted': deducted,
                'not_covered': to_deduct - deducted,
                'remaining': remaining,
            }
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def deduct(self, org_cost_delta, month):
        for org_id, cost_delta in org_cost_delta.items():
            if org_id <= 0:
                continue
            try:
                result = self.deduct_overflow_credits(org_id, cost_delta, month)
            except Exception as e:
                logger.exception('Failed to deduct additional AI credits for org %s: %s', org_id, e)
                continue
            if result:
                logger.info('Deduct additional AI credits for org %s monthly_credit %s overflow %s deducted %s not_covered %s remaining %s',
                            org_id, result['monthly_credit'], result['overflow'], result['deducted'],
                            result['not_covered'], result['remaining'])
