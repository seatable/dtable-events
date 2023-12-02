import logging
from datetime import datetime, timedelta
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.db import init_db_session_class
from dtable_events.utils import utc_to_tz
from dtable_events.app.config import TIME_ZONE

logger = logging.getLogger(__name__)

__all__ = [
    'DTableAssetTrashCleaner',
]


class DTableAssetTrashCleaner(object):

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
            logging.warning('Can not start dtable asset trash cleaner: it is not enabled!')
            return

        logging.info('Start dtable asset trash cleaner, expire days: %s', self._expire_days)

        DTableAssetTrashCleanerTimer(self._db_session_class, self._expire_days).start()

    def is_enabled(self):
        return self._enabled


class DTableAssetTrashCleanerTimer(Thread):

    def __init__(self, db_session_class, expire_days):
        super(DTableAssetTrashCleanerTimer, self).__init__()
        self.db_session_class = db_session_class
        self.expire_days = expire_days

    def run(self):
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0')
        def timed_job():
            logging.info('Starts to clean dtable asset trash...')

            db_session = self.db_session_class()

            inactive_time_limit = utc_to_tz(datetime.utcnow(), TIME_ZONE) - timedelta(days=self.expire_days)

            sql = '''
                DELETE FROM dtable_asset_trash
                WHERE deleted_at <= :inactive_time_limit
            '''

            try:
                db_session.execute(text(sql), {'inactive_time_limit': inactive_time_limit})
                db_session.commit()
            except Exception as e:
                logging.exception('error when cleaning dtable asset trash: %s', e)
            finally:
                db_session.close()

        sched.start()
