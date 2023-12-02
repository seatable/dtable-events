import logging
from datetime import datetime, timedelta
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.metadata_cache_managers import RuleIntervalMetadataCacheManager
from dtable_events.automations.auto_rules_utils import run_regular_execution_rule
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool


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
        section_name = 'AUTOMATION-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'AUTOMATION SCANNER'
            if not config.has_section(section_name):
                return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable automation rules scanner: it is not enabled!')
            return

        logging.info('Start dtable automation rules scanner')

        DTableAutomationRulesScannerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def scan_dtable_automation_rules(db_session):
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
    rules = db_session.execute(text(sql), {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time,
        'per_month_check_time': per_month_check_time
    })

    # each base's metadata only requested once and recorded in memory
    # The reason why it doesn't cache metadata in redis is metadatas in interval rules need to be up-to-date
    rule_interval_metadata_cache_manager = RuleIntervalMetadataCacheManager()
    for rule in rules:
        try:
            run_regular_execution_rule(rule, db_session, rule_interval_metadata_cache_manager)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')
        db_session.commit()


class DTableAutomationRulesScannerTimer(Thread):

    def __init__(self, db_session_class):
        super(DTableAutomationRulesScannerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan automation rules...')

            db_session = self.db_session_class()
            try:
                scan_dtable_automation_rules(db_session)
            except Exception as e:
                logging.exception('error when scanning dtable automation rules: %s', e)
            finally:
                db_session.close()

        sched.start()
