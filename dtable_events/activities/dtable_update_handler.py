import logging
from datetime import datetime
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler
from dtable_events.db import init_db_session_class

class DTableUpdateHander(object):

    def __init__(self, app, config):
        self.app = app
        self._enabled = True
        self._db_session_class = init_db_session_class(config)

    def start(self):
        logging.info('Start dtable update scanner')
        DTableUpdateTimer(self._db_session_class, self.app).start()

    def is_enabled(self):
        return self._enabled


def update_dtable_updated_at_time(db_session, app):
    cache = app.dtable_update_cache
    time_dict = cache.updated_time_dict
    if not time_dict:
        return
    for dtable_uuid, update_timestamp in time_dict.items():
        try:
            dt = datetime.fromtimestamp(update_timestamp)
            set_updated_time_sql = '''UPDATE dtables SET updated_at=:dt WHERE uuid=:dtable_uuid'''
            db_session.execute(text(set_updated_time_sql), {
                'dt': dt,
                'dtable_uuid': dtable_uuid 
            })
            db_session.commit()
        except Exception as e:
            logging.exception('error when updated base update tiems: %s', e)
            continue
    cache.clean_dtable_update_time_info()


class DTableUpdateTimer(Thread):

    def __init__(self, db_session_class, app):
        super(DTableUpdateTimer, self).__init__()
        self.db_session_class = db_session_class
        self.app = app

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', minute="*/10")
        def timed_job():
            logging.info('Starts to scan updated bases')
            db_session = self.db_session_class()
            try:
                update_dtable_updated_at_time(db_session, self.app)
            except Exception as e:
                logging.exception('error when scanning updated bases: %s', e)
            finally:
                db_session.close()

        sched.start()
