import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_python_executable, run
from dtable_events.app.config import dtable_web_dir

try:
    from seahub.settings import ENABLE_SUB_ACCOUNT
except ImportError as err:
    ENABLE_SUB_ACCOUNT = False

logger = logging.getLogger(__name__)

__all__ = [
    'InactiveExpiredSubAccountsWorker',
]


class InactiveExpiredSubAccountsWorker(object):

    def __init__(self, config):
        self._enabled = ENABLE_SUB_ACCOUNT
        self._interval = 12 * 3600  # 12h

    def start(self):
        if not self._enabled:
            logging.warning('Can not start inactive expired sub accounts: it is not enabled!')
            return
        logging.info('Start inactive expired sub accounts.')

        InactiveExpiredSubAccountsWorkerTimer(interval=self._interval).start()


class InactiveExpiredSubAccountsWorkerTimer(Thread):

    def __init__(self, interval):
        Thread.__init__(self)
        self._interval = interval
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts inactive expired sub accounts...')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')

                    cmd = [
                        python_exec,
                        manage_py,
                        'inactive_expired_sub_accounts',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when inactive expired sub accounts: %s', e)

    def cancel(self):
        self.finished.set()
