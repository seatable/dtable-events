import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_opt_from_conf_or_env, get_python_executable, run


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')

__all__ = [
    'DTablesCleaner',
]


class DTablesCleaner(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60 * 24
        self._prepare_logfile()
        self._parse_config(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'dtables_cleaner.log')

    def _parse_config(self, config):
        self._expire_seconds = 60 * 60 * 24 * 30

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtables cleaner: it is not enabled!')
            return

        logging.info('Start dtables cleaner, interval = %s sec', self._interval)

        DTablesCleanerTimer(self._interval, self._logfile, self._expire_seconds).start()

    def is_enabled(self):
        return self._enabled


class DTablesCleanerTimer(Thread):

    def __init__(self, interval, logfile, 
                expire_seconds=30*60):
        super(DTablesCleanerTimer, self).__init__()
        self._interval = interval
        self._logfile = logfile
        self._expire_seconds = expire_seconds

        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to clean trash dtables...')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'clean_trash_dtables',
                        self._expire_seconds,
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when cleaning trash dtables: %s', e)

    def cancel(self):
        self.finished.set()
