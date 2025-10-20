import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from queue import Queue, Empty
from threading import Thread, current_thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.log import auto_rule_logger
from dtable_events.app.metadata_cache_managers import RuleIntervalMetadataCacheManager
from dtable_events.automations.auto_rules_utils import run_regular_execution_rule
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool
from dtable_events.utils.utils_metric import publish_metric, SCHEDULED_AUTOMATION_RULES_QUEUE_METRIC_HELP, \
    SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP


__all__ = [
    'DTableAutomationRulesScanner',
]

class DTableAutomationRulesScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._parse_config(config)
        self._db_session_class = init_db_session_class(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AUTOMATION'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self.is_enabled():
            auto_rule_logger.warning('Can not start dtable automation rules scanner: it is not enabled!')
            return

        auto_rule_logger.info('Start dtable automation rules scanner')

        DTableAutomationRulesScannerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class DTableAutomationRulesScannerTimer(Thread):

    def __init__(self, db_session_class):
        super(DTableAutomationRulesScannerTimer, self).__init__()
        self.db_session_class = db_session_class
        self.max_workers = 3
        self.queue = Queue()

        self.trigger_count = 0

    def trigger_rule(self):
        auto_rule_logger.info('thread %s start', current_thread().name)
        rule_interval_metadata_cache_manager = RuleIntervalMetadataCacheManager()
        while True:
            try:
                rule = self.queue.get(timeout=1)
            except Empty:
                return  # means no rules to trigger, finish thread
            publish_metric(self.queue.qsize(), 'scheduled_automation_queue_size', SCHEDULED_AUTOMATION_RULES_QUEUE_METRIC_HELP)
            db_session = self.db_session_class()
            auto_rule_logger.info('thread %s start to handle rule %s dtable_uuid %s', current_thread().name, rule.id, rule.dtable_uuid)
            try:
                run_regular_execution_rule(rule, db_session, rule_interval_metadata_cache_manager)
            except Exception as e:
                auto_rule_logger.exception(e)
                auto_rule_logger.error(f'check rule failed. {rule}, error: {e}')
            finally:
                db_session.close()

    def scan_dtable_automation_rules(self):
        sql = '''
                SELECT `dar`.`id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid`, `trigger_count`, `org_id`, dar.`creator` FROM dtable_automation_rules dar
                JOIN dtables d ON dar.dtable_uuid=d.uuid
                WHERE ((run_condition='per_day' AND (last_trigger_time<:per_day_check_time OR last_trigger_time IS NULL))
                OR (run_condition='per_week' AND (last_trigger_time<:per_week_check_time OR last_trigger_time IS NULL))
                OR (run_condition='per_month' AND (last_trigger_time<:per_month_check_time OR last_trigger_time IS NULL)))
                AND dar.is_valid=1 AND d.deleted=0 AND is_pause=0
            '''
        per_day_check_time = datetime.utcnow() - timedelta(hours=23)
        per_week_check_time = datetime.utcnow() - timedelta(days=6)
        per_month_check_time = datetime.utcnow() - timedelta(days=27)  # consider the least month-days 28 in February (the 2nd month) in common years
        db_session = self.db_session_class()
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

        for rule in rules:
            self.trigger_count += 1
            self.queue.put(rule)
        publish_metric(self.queue.qsize(), 'scheduled_automation_queue_size', SCHEDULED_AUTOMATION_RULES_QUEUE_METRIC_HELP)
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='interval-auto-rules') as executor:
            for _ in range(self.max_workers):
                executor.submit(self.trigger_rule)

        publish_metric(self.trigger_count, 'scheduled_automation_triggered_count', SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)

        auto_rule_logger.info('all rules done')

    def run(self):
        publish_metric(self.trigger_count, 'scheduled_automation_triggered_count', SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*', misfire_grace_time=600)
        def timed_job():
            auto_rule_logger.info('Starts to scan automation rules...')

            publish_metric(self.queue.qsize(), 'scheduled_automation_queue_size', SCHEDULED_AUTOMATION_RULES_QUEUE_METRIC_HELP)
            try:
                self.scan_dtable_automation_rules()
            except Exception as e:
                auto_rule_logger.exception('error when scanning dtable automation rules: %s', e)

        sched.start()
