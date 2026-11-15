import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_python_executable, run
from dtable_events.app.config import dtable_web_dir, TRASH_CLEAN_AFTER_DAYS

__all__ = [
    'SeaDBDatabasesCleaner',
]


class SeaDBDatabasesCleaner(object):

    def __init__(self):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60 * 24
        self._prepare_logfile()
        self._parse_config()

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'seadb_databases_cleaner.log')

    def _parse_config(self):
        self._expire_seconds = 60 * 60 * 24 * TRASH_CLEAN_AFTER_DAYS

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start seadb databases cleaner: it is not enabled!')
            return

        logging.info('Start seadb databases cleaner, interval = %s sec', self._interval)

        SeaDBDatabasesCleanerTimer(self._interval, self._logfile, self._expire_seconds).start()

    def is_enabled(self):
        return self._enabled


class SeaDBDatabasesCleanerTimer(Thread):

    def __init__(self, interval, logfile,
                expire_seconds=30*60):
        super(SeaDBDatabasesCleanerTimer, self).__init__()
        self._interval = interval
        self._logfile = logfile
        self._expire_seconds = expire_seconds

        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to clean trash seadb databases...')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'clean_trash_seadb_databases',
                        self._expire_seconds,
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when cleaning trash seadb databases: %s', e)

    def cancel(self):
        self.finished.set()
