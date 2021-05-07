import os
import sys
import logging
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.activities.notification_rules_utils import check_near_deadline_notification_rule
from dtable_events.app.event_redis import RedisClient
from dtable_events.cache.clients import RedisCacheClient
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, get_python_executable, run


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

# CONF DIR
central_conf_dir, timezone = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', ''), 'UTC'
if central_conf_dir:
    sys.path.insert(0, central_conf_dir)
    try:
        import dtable_web_settings
        timezone = getattr(dtable_web_settings, 'TIME_ZONE', 'UTC')
    except Exception as e:
        logging.error('import dtable_web_settings error: %s', e)
    else:
        del dtable_web_settings
else:
    logging.error('no conf dir SEAFILE_CENTRAL_CONF_DIR find')


__all__ = [
    'DTableNofiticationRulesScanner',
]


class DTableNofiticationRulesScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._config = config
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

        DTableNofiticationRulesScannerTimer(self._logfile, self._db_session_class, self._config).start()

    def is_enabled(self):
        return self._enabled


def scan_dtable_notification_rules(db_session, timezone, cache=None):
    sql = '''
            SELECT `id`, `trigger`, `action`, `creator`, `last_trigger_time`, `dtable_uuid` FROM dtable_notification_rules
            WHERE (run_condition='per_day' AND last_trigger_time<:per_day_check_time)
            OR (run_condition='per_week' AND last_trigger_time<:per_week_check_time)
            OR last_trigger_time is null
        '''
    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    per_week_check_time = datetime.utcnow() - timedelta(days=6)
    rules = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time
    })

    for rule in rules:
        if not rule[5]:  # filter and ignore non-dtable-uuid records(some old records)
            continue
        try:
            check_near_deadline_notification_rule(rule, db_session, timezone, cache=cache)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')
        db_session.commit()


class DTableNofiticationRulesScannerTimer(Thread):

    def __init__(self, logfile, db_session_class, config):
        super(DTableNofiticationRulesScannerTimer, self).__init__()
        self._logfile = logfile
        self.db_session_class = db_session_class
        self._config = config

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan notification rules...')

            db_session = self.db_session_class()
            _redis_client = RedisClient(self._config)
            cache = RedisCacheClient(_redis_client)
            try:
                scan_dtable_notification_rules(db_session, timezone, cache=cache)
            except Exception as e:
                logging.exception('error when scanning dtable notification rules: %s', e)
            finally:
                db_session.close()
                # disconnect manually, because this task runs only once per hour
                _redis_client.connection.connection_pool.disconnect()

        sched.start()

