from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from threading import current_thread

from sqlalchemy import text

from dtable_events.app.metadata_cache_managers import RuleIntervalMetadataCacheManager
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.auto_rules_utils import scan_triggered_automation_rules, run_regular_execution_rule
from dtable_events.celery_app.app import app
from dtable_events.celery_app.tasks.base import DatabaseTask


@app.task(bind=True, base=DatabaseTask)
def trigger_automation_rule(self, event_data):
    try:
        scan_triggered_automation_rules(event_data, self.db_session)
    except Exception as e:
        auto_rule_logger.exception(e)


def trigger_rule(db_session_class, queue):
    auto_rule_logger.info('thread %s start', current_thread().name)
    rule_interval_metadata_cache_manager = RuleIntervalMetadataCacheManager()
    while True:
        try:
            rule = queue.get(timeout=1)
        except Empty:
            return  # means no rules to trigger, finish thread
        db_session = db_session_class()
        auto_rule_logger.info('thread %s start to handle rule %s dtable_uuid %s', current_thread().name, rule.id, rule.dtable_uuid)
        try:
            run_regular_execution_rule(rule, db_session, rule_interval_metadata_cache_manager)
        except Exception as e:
            auto_rule_logger.exception(e)
            auto_rule_logger.error(f'check rule failed. {rule}, error: {e}')
        finally:
            db_session.close()


@app.task(bind=True, base=DatabaseTask)
def scan_automation_rules(self):
    sql = '''
            SELECT `dar`.`id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid`, `trigger_count`, `org_id`, dar.`creator` FROM dtable_automation_rules dar
            JOIN dtables d ON dar.dtable_uuid=d.uuid
            WHERE ((run_condition='per_day' AND (last_trigger_time<:per_day_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_week' AND (last_trigger_time<:per_week_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_month' AND (last_trigger_time<:per_month_check_time OR last_trigger_time IS NULL)))
            AND dar.is_valid=1 AND d.deleted=0 AND is_pause=0
        '''
    per_day_check_time = datetime.now(timezone.utc) - timedelta(hours=23)
    per_week_check_time = datetime.now(timezone.utc) - timedelta(days=6)
    per_month_check_time = datetime.now(timezone.utc) - timedelta(days=27)  # consider the least month-days 28 in February (the 2nd month) in common years
    db_session = self.db_session
    try:
        rules = db_session.execute(text(sql), {
            'per_day_check_time': per_day_check_time,
            'per_week_check_time': per_week_check_time,
            'per_month_check_time': per_month_check_time
        })
    except Exception as e:
        auto_rule_logger.exception('query regular automation rules error: %s', e)
        return
    finally:
        db_session.close()

    queue = Queue()
    max_workers = 3
    for rule in rules:
        queue.put(rule)
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='interval-auto-rules') as executor:
        for _ in range(max_workers):
            executor.submit(trigger_rule, app.db_session_class, queue)

    auto_rule_logger.info('all rules done')
