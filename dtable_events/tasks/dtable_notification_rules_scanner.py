import os
import logging
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, get_python_executable, run


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

__all__ = [
    'DTableNofiticationRulesScanner',
]


class DTableNofiticationRulesScanner(object):

    def __init__(self, config):
        self._enabled = False
        self._logfile = None
        self._parse_config(config)
        self._prepare_logfile()

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
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self._enabled = True


    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable notification rules scanner: it is not enabled!')
            return

        logging.info('Start dtable notification rules scanner')

        DTableNofiticationRulesScannerTimer(self._logfile).start()

    def is_enabled(self):
        return self._enabled


class DTableNofiticationRulesScannerTimer(Thread):

    def __init__(self, logfile):
        super(DTableNofiticationRulesScannerTimer, self).__init__()
        self._logfile = logfile

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan notification rules...')
            try:
                python_exec = get_python_executable()
                manage_py = os.path.join(dtable_web_dir, 'manage.py')
                cmd = [
                    python_exec,
                    manage_py,
                    'scan_dtable_notification_rules',
                ]
                with open(self._logfile, 'a') as fp:
                    run(cmd, cwd=dtable_web_dir, output=fp)
            except Exception as e:
                logging.exception('error when scanning dtable notification rules: %s', e)

        sched.start()

