from dtable_events import init_db_session_class
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.actions import AutomationRule, auto_rule_logger

def run_auto_rule_task(trigger, actions, options, config):
    db_session = init_db_session_class(config)()
    try:
        auto_rule_logger.info('start to run test auto rule: %s', options['rule_id'])
        auto_rule = AutomationRule(None, trigger, actions, options)
        auto_rule.do_actions(db_session, with_test=True)
    except Exception as e:
        auto_rule_logger.exception('automation rule: %s run test error: %s', options['rule_id'], e)
    finally:
        db_session.close()
