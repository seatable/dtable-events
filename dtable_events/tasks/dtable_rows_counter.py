import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_opt_from_conf_or_env, get_python_executable, run, parse_bool
from dtable_events.app.config import dtable_web_dir

__all__ = [
    'DTableRowsCounter',
]


class DTableRowsCounter(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._interval = 24 * 60 * 60
        self._prepare_logfile()
        self._prepara_config(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'dtable_rows_counter.log')

    def _prepara_config(self, config):
        section_name = 'ROWS-COUNTER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        self._enabled = parse_bool(enabled)

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable rows count')
            return
        logging.info('Start dtable rows count...')
        DTableRowsCounterTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class DTableRowsCounterTimer(Thread):

    def __init__(self, interval, logfile):
        super(DTableRowsCounterTimer, self).__init__()
        self._interval = interval
        self._logfile = logfile

        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to count rows of users or organizations')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'count_user_org_rows'
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when counting rows: %s', e)

    def cancel(self):
        self.finished.set()
