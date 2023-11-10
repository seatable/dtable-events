import os
import logging
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import TIME_ZONE
from dtable_events.app.metadata_cache_managers import RuleIntervalMetadataCacheManager
from dtable_events.db import init_db_session_class
from dtable_events.notification_rules.notification_rules_utils import trigger_near_deadline_notification_rule
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool


timezone = TIME_ZONE


__all__ = [
    'DTableNofiticationRulesScanner',
]


class DTableNofiticationRulesScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._parse_config(config)
        self._prepare_logfile()
        self._db_session_class = init_db_session_class(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'dtables_notification_rule_scanner.log')

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'NOTIFY-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'NOTIFY SCANNER'
            if not config.has_section(section_name):
                return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable notification rules scanner: it is not enabled!')
            return

        logging.info('Start dtable notification rules scanner')

        DTableNofiticationRulesScannerTimer(self._logfile, self._db_session_class).start()
        DTableNotificationRulesCleaner(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def scan_dtable_notification_rules(db_session):
    sql = '''
            SELECT `dnr`.`id`, `trigger`, `action`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules dnr
            JOIN dtables d ON dnr.dtable_uuid=d.uuid
            WHERE ((run_condition='per_day' AND (last_trigger_time<:per_day_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_week' AND (last_trigger_time<:per_week_check_time OR last_trigger_time IS NULL)))
            AND is_valid=1 AND d.deleted=0
        '''
    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    per_week_check_time = datetime.utcnow() - timedelta(days=6)
    rules = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time
    })
    # each base's metadata only requested once and recorded in memory
    # The reason why it doesn't cache metadata in redis is metadatas in interval rules need to be up-to-date
    rule_interval_metadata_cache_manager = RuleIntervalMetadataCacheManager()
    for rule in rules:
        if not rule[4]:  # filter and ignore non-dtable-uuid records(some old records)
            continue
        try:
            trigger_near_deadline_notification_rule(rule, db_session, rule_interval_metadata_cache_manager)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')
        db_session.commit()


class DTableNofiticationRulesScannerTimer(Thread):

    def __init__(self, logfile, db_session_class):
        super(DTableNofiticationRulesScannerTimer, self).__init__()
        self._logfile = logfile
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan notification rules...')

            db_session = self.db_session_class()
            try:
                scan_dtable_notification_rules(db_session)
            except Exception as e:
                logging.exception('error when scanning dtable notification rules: %s', e)
            finally:
                db_session.close()

        sched.start()


class DTableNotificationRulesCleaner(Thread):
    def __init__(self, db_session_class):
        super(DTableNotificationRulesCleaner, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0')
        def timed_job():
            logging.info('Starts to clean inactive notification rules...')

            db_session = self.db_session_class()

            inactive_time_limit = datetime.utcnow() - timedelta(days=180)

            # update rules that are only created but not triggered for too long or not triggered for too long is_valid=0
            sql = '''
                UPDATE dtable_notification_rules
                SET is_valid=0
                WHERE (last_trigger_time IS NULL AND ctime < :inactive_time_limit)
                OR (last_trigger_time IS NOT NULL AND last_trigger_time < :inactive_time_limit)
            '''

            try:
                db_session.execute(sql, {'inactive_time_limit': inactive_time_limit})
                db_session.commit()
            except Exception as e:
                logging.exception('error when cleaning inactive notification rules: %s', e)
            finally:
                db_session.close()

        sched.start()
