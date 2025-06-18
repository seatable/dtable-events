import os
import logging
from threading import Thread
from apscheduler.schedulers.blocking import BlockingScheduler

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
        self._logfile = None
        self._prepare_logfile()

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'inactive_expired_sub_account.log')

    def start(self):
        if not self._enabled:
            logging.info('Can not start inactive expired sub accounts: it is not enabled!')
            return
        logging.info('Start inactive expired sub accounts.')

        InactiveExpiredSubAccountsWorkerTimer(self._logfile).start()


class InactiveExpiredSubAccountsWorkerTimer(Thread):

    def __init__(self, logfile):
        Thread.__init__(self)
        self._logfile = logfile

    def run(self):
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0', minute='1', misfire_grace_time=600)
        def timed_job():
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

        sched.start()
