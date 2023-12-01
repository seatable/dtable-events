import json
import logging

from seaserv import ccnet_api

from dtable_events import init_db_session_class
from dtable_events.app.event_redis import redis_cache
from dtable_events.app.metadata_cache_managers import RuleIntentMetadataCacheManger, RuleIntervalMetadataCacheManager
from dtable_events.automations.actions import AutomationRule
from dtable_events.utils import uuid_str_to_32_chars, uuid_str_to_36_chars

logger = logging.getLogger(__name__)


def get_role_cache_key(name):
    return f'role:{name}'


def get_user_role_cache_key(username):
    return f'user:{username}:role'


def get_org_role_cache_key(org_id):
    return f'org:{org_id}:role'


def get_user_trigger_cache_key(username):
    return f'user:{username}:trigger_count'


def get_org_trigger_cache_key(org_id):
    return f'org:{org_id}:trigger_count'


def get_dtable_workspace_cache_key(dtable_uuid):
    return f'dtable:{uuid_str_to_36_chars(dtable_uuid)}:workspace'


def get_owner_and_org_id(dtable_uuid, db_session):
    dtable_workspace_cache_key = get_dtable_workspace_cache_key(dtable_uuid)
    workspace_cache = redis_cache.get()
    if not workspace_cache:
        sql = "SELECT w.owner, w.org_id FROM workspaces w JOIN dtables d ON w.id=d.workspace_id WHERE d.uuid=:dtable_uuid"
        workspace = db_session.execute(sql, {'dtable_uuid': uuid_str_to_32_chars(dtable_uuid)}).fetchone()
        if not workspace:
            raise Exception(404, 'dtable: %s workspace not found' % dtable_uuid)
        redis_cache.set(dtable_workspace_cache_key, json.dumps({'owner': workspace.owner, 'org_id': workspace.org_id}), timeout=24*60*60)
    else:
        workspace = json.loads(workspace_cache)
        owner, org_id = workspace['owner'], workspace['org_id']
    return owner, org_id


def get_role(owner, org_id, db_session):
    role = None
    if org_id == -1:  # get user role
        user_role_cache_key = get_user_role_cache_key(owner)
        user_role_cache = redis_cache.get(user_role_cache_key)
        if not user_role_cache:
            user = ccnet_api.get_emailuser(owner)
            if not user:
                raise Exception(404, 'user %s not found' % owner)
            role = user.role
            redis_cache.set(user_role_cache_key, role, timeout=24*60*60)
        else:
            role = user_role_cache
    else:
        org_role_cache_key = get_org_role_cache_key(org_id)
        org_role_cache = redis_cache.get(org_role_cache_key)
        if not org_role_cache:
            sql = "SELECT role FROM organizations_orgsettings WHERE org_id=:org_id"
            org_settings = db_session.execute(sql, {'org_id': org_id}).fetchone()
            if not org_settings:
                raise Exception(404, 'org %s not found' % org_id)
            role = org_settings.role
            redis_cache.set(org_role_cache_key, role, timeout=24*60*60)
        else:
            role = org_role_cache
    return role


def get_trigger_count(owner, org_id, db_session):
    if org_id == -1:
        user_trigger_cache_key = get_user_trigger_cache_key(owner)
        user_trigger_cache = redis_cache.get(user_trigger_cache_key)
        if not user_trigger_cache:
            sql = "SELECT trigger_count FROM user_auto_rules_statistics WHERE username=:username"
            stats = db_session.execute(sql, {'username': owner}).fetchone()
            trigger_count = stats.trigger_count if stats else 0
            redis_cache.set(user_trigger_cache_key, trigger_count, timeout=30*60)
        else:
            trigger_count = int(user_trigger_cache)
    else:
        org_trigger_cache_key = get_org_trigger_cache_key(org_id)
        org_trigger_cache = redis_cache.get(org_trigger_cache_key)
        if not org_trigger_cache:
            sql = "SELECT trigger_count FROM org_auto_rules_statistics WHERE org_id=:org_id"
            stats = db_session.execute(sql, {'org_id': org_id}).fetchone()
            trigger_count = stats.trigger_count if stats else 0
            redis_cache.set(org_trigger_cache_key, trigger_count, timeout=30*60)
        else:
            trigger_count = int(user_trigger_cache)
    return trigger_count


def get_limit(role):
    role_cache = get_role_cache_key(role)
    if not role_cache:
        logger.warning('no role cache found')
        return True
    role_settings = json.loads(role_cache)
    return role_settings.get('automation_rules_limit_per_month')


def can_trigger_by_dtable(dtable_uuid, db_session):
    # get owner org_id
    owner, org_id = get_owner_and_org_id(dtable_uuid, db_session)
    if org_id == -1:
        if '@seafile_group' in owner:  # group not belong to org always can trigger rules
            return True
    # get owner org_id role
    role = get_role(owner, org_id, db_session)
    # get trigger_count
    trigger_count = get_trigger_count(owner, org_id, db_session)
    # get limit
    limit_per_month = get_limit(role)
    if not limit_per_month:
        return True
    if limit_per_month < 0:
        return True
    return limit_per_month > trigger_count


def scan_triggered_automation_rules(event_data, db_session, per_minute_trigger_limit):
    dtable_uuid = event_data.get('dtable_uuid')
    automation_rule_id = event_data.get('automation_rule_id')
    sql = """
        SELECT `id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid`, `trigger_count`, `org_id`, `creator` FROM `dtable_automation_rules`
        WHERE dtable_uuid=:dtable_uuid AND run_condition='per_update' AND is_valid=1 AND id=:rule_id AND is_pause=0
    """

    try:
        rule = db_session.execute(sql, {'dtable_uuid': dtable_uuid, 'rule_id': automation_rule_id}).fetchone()
    except Exception as e:
        logger.error('checkout auto rules error: %s', e)
        return
    if not rule:
        return

    rule_intent_metadata_cache_manager = RuleIntentMetadataCacheManger()
    options = {
        'rule_id': rule.id,
        'run_condition': rule.run_condition,
        'dtable_uuid': dtable_uuid,
        'trigger_count': rule.trigger_count,
        'org_id': rule.org_id,
        'creator': rule.creator,
        'last_trigger_time': rule.last_trigger_time,
    }
    try:
        auto_rule = AutomationRule(event_data, db_session, rule.trigger, rule.actions, options, rule_intent_metadata_cache_manager, per_minute_trigger_limit=per_minute_trigger_limit)
        auto_rule.do_actions()
    except Exception as e:
        logger.error('auto rule: %s do actions error: %s', rule.id, e)


def run_regular_execution_rule(rule, db_session, metadata_cache_manager):
    trigger = rule[2]
    actions = rule[3]

    options = {}
    options['rule_id'] = rule[0]
    options['run_condition'] = rule[1]
    options['last_trigger_time'] = rule[4]
    options['dtable_uuid'] = rule[5]
    options['trigger_count'] = rule[6]
    options['org_id'] = rule[7]
    options['creator'] = rule[8]
    try:
        auto_rule = AutomationRule(None, db_session, trigger, actions, options, metadata_cache_manager)
        auto_rule.do_actions()
    except Exception as e:
        logger.error('auto rule: %s do actions error: %s', options['rule_id'], e)

def run_auto_rule_task(trigger, actions, options, config):
    from dtable_events.automations.actions import AutomationRule
    db_session = init_db_session_class(config)()
    metadata_cache_manager = RuleIntervalMetadataCacheManager()
    try:
        auto_rule = AutomationRule(None, db_session, trigger, actions, options, metadata_cache_manager)
        auto_rule.do_actions(with_test=True)
    except Exception as e:
        logger.error('automation rule run test error: {}'.format(e))
    finally:
        db_session.close()
