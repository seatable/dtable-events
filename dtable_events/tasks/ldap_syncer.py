import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_opt_from_conf_or_env, \
     get_python_executable, run_and_wait, parse_bool, parse_interval
from dtable_events.app.config import dtable_web_dir


class LDAPSyncer(object):

    def __init__(self, config):
        self._enabled = False
        self._logfile = None
        self._interval = 60 * 60
        self._prepare_logfile()
        self._prepara_config(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'ldap_syncer.log')

    def _prepara_config(self, config):
        section_name = 'LDAP_SYNC'
        key_enabled = 'enabled'
        key_sync_interval = 'sync_interval'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        self._enabled = parse_bool(enabled)
        interval = get_opt_from_conf_or_env(config, section_name, key_sync_interval, default=60*60)
        self._interval = parse_interval(interval, 60*60)

    def start(self):
        if not self.is_enabled():
            logging.warning('LDAP syncer not enabled')
            return
        logging.info('Start ldap syncer')
        LDAPSyncerTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class LDAPSyncerTimer(Thread):

    def __init__(self, interval, logfile):
        super(LDAPSyncerTimer, self).__init__()
        self._interval = interval
        self._logfile = logfile

        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to ldap sync')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'ldap_user_sync'
                    ]
                    with open(self._logfile, 'a') as fp:
                        run_and_wait(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when sync ldap user: %s', e)

                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'ldap_group_sync'
                    ]
                    with open(self._logfile, 'a') as fp:
                        run_and_wait(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when sync ldap group: %s', e)

    def cancel(self):
        self.finished.set()
