import logging

from sqlalchemy import text

from dtable_events import init_db_session_class
from dtable_events.app.metadata_cache_managers import RuleInstantMetadataCacheManger, RuleIntervalMetadataCacheManager
from dtable_events.automations.actions import AutomationRule, auto_rule_logger

logger = logging.getLogger(__name__)


def scan_triggered_automation_rules(event_data, db_session, per_minute_trigger_limit):
    # if event_data.get('op_user') == 'Automation Rule':
    #     # For preventing loop do automation actions, foribidden triggering actions!!!
    #     return
    dtable_uuid = event_data.get('dtable_uuid')
    automation_rule_id = event_data.get('automation_rule_id')
    sql = """
        SELECT `id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid`, `trigger_count`, `org_id`, `creator` FROM `dtable_automation_rules`
        WHERE dtable_uuid=:dtable_uuid AND run_condition='per_update' AND is_valid=1 AND id=:rule_id AND is_pause=0
    """

    try:
        rules = db_session.execute(text(sql), {'dtable_uuid': dtable_uuid, 'rule_id': automation_rule_id}).fetchall()
    except Exception as e:
        logger.error('checkout auto rules error: %s', e)
        return

    rule_instant_metadata_cache_manager = RuleInstantMetadataCacheManger()
    for rule_id, run_condition, trigger, actions, last_trigger_time, dtable_uuid, trigger_count, org_id, creator in rules:
        options = {
            'rule_id': rule_id,
            'run_condition': run_condition,
            'dtable_uuid': dtable_uuid,
            'trigger_count': trigger_count,
            'org_id': org_id,
            'creator': creator,
            'last_trigger_time': last_trigger_time,
        }
        try:
            auto_rule_logger.info('run auto rule %s', rule_id)
            auto_rule = AutomationRule(event_data, db_session, trigger, actions, options, rule_instant_metadata_cache_manager, per_minute_trigger_limit=per_minute_trigger_limit)
            auto_rule.do_actions()
        except Exception as e:
            logger.exception('auto rule: %s do actions error: %s', rule_id, e)
            auto_rule_logger.exception('auto rule: %s do actions error: %s', rule_id, e)


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
        auto_rule_logger.info('start to run regular auto rule: %s', options['rule_id'])
        auto_rule = AutomationRule(None, db_session, trigger, actions, options, metadata_cache_manager)
        auto_rule.do_actions()
    except Exception as e:
        logger.exception('auto rule: %s do actions error: %s', options['rule_id'], e)
        auto_rule_logger.exception('auto rule: %s do actions error: %s', options['rule_id'], e)

def run_auto_rule_task(trigger, actions, options, config):
    from dtable_events.automations.actions import AutomationRule
    db_session = init_db_session_class(config)()
    metadata_cache_manager = RuleIntervalMetadataCacheManager()
    try:
        auto_rule_logger.info('start to run test auto rule: %s', options['rule_id'])
        auto_rule = AutomationRule(None, db_session, trigger, actions, options, metadata_cache_manager)
        auto_rule.do_actions(with_test=True)
    except Exception as e:
        logger.exception('automation rule: %s run test error: %s', options['rule_id'], e)
        auto_rule_logger.exception('automation rule: %s run test error: %s', options['rule_id'], e)
    finally:
        db_session.close()
