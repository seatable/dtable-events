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

    dtable_uuids = list(time_dict.keys())
    step = 100
    for i in range(0, len(dtable_uuids), step):
        step_dtable_uuids = []
        case_phrases = []
        for dtable_uuid in dtable_uuids[i: i+step]:
            dt = datetime.fromtimestamp(time_dict[dtable_uuid])
            case_item = '''WHEN uuid = '%s' THEN '%s' ''' % (dtable_uuid, str(dt))
            case_phrases.append(case_item)
            step_dtable_uuids.append("'%s'" % dtable_uuid)
        case_phrases.append('''ELSE updated_at''')
        try:
            dtable_uuid_str = ', '.join(step_dtable_uuids)
            batch_set_updated_time_sql = '''
                UPDATE dtables 
                SET updated_at = CASE
                %(cases)s
                END
                WHERE uuid in (%(dtable_uuids)s)
                ''' % ({
                    'cases': ' '.join(case_phrases),
                    'dtable_uuids': dtable_uuid_str
                })
        
            db_session.execute(text(batch_set_updated_time_sql))
            db_session.commit()
        except Exception as e:
            logging.exception('error when updated base update time error: %s', e)
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
