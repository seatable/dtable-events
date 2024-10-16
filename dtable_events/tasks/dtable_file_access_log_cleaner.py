import logging
from datetime import datetime, timedelta
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import TIME_ZONE
from dtable_events.db import init_db_session_class
from dtable_events.utils import utc_to_tz

logger = logging.getLogger(__name__)

__all__ = [
    'DTableFileAccessLogCleaner',
]


class DTableFileAccessLogCleaner(object):

    def __init__(self, config):
        self._enabled = True
        self._db_session_class = init_db_session_class(config)
        self._enabled = False
        self._expire_days = 60
        self._parse_config()

    def _parse_config(self):
        self._enabled = True
        self._expire_days = 60

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable file access log cleaner: it is not enabled!')
            return

        logging.info('Start dtable file access log cleaner, expire days: %s', self._expire_days)

        DTableFileAccessLogCleanerTimer(self._db_session_class, self._expire_days).start()

    def is_enabled(self):
        return self._enabled


class DTableFileAccessLogCleanerTimer(Thread):

    def __init__(self, db_session_class, expire_days):
        super(DTableFileAccessLogCleanerTimer, self).__init__()
        self.db_session_class = db_session_class
        self.expire_days = expire_days

    def run(self):
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            logging.info('Starts to clean dtable file access log...')

            db_session = self.db_session_class()

            inactive_time_limit = utc_to_tz(datetime.utcnow(), TIME_ZONE) - timedelta(days=self.expire_days)

            sql = "DELETE FROM `file_access_log` WHERE `timestamp` <= :inactive_time_limit"

            try:
                db_session.execute(text(sql), {'inactive_time_limit': inactive_time_limit})
                db_session.commit()
            except Exception as e:
                logging.exception('error when cleaning dtable file access log: %s', e)
            finally:
                db_session.close()

        sched.start()
