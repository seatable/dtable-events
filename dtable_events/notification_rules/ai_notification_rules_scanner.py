import json
import os
import logging
from datetime import datetime
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import TIME_ZONE, DTABLE_WEB_SERVICE_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils.dtable_web_api import DTableWebAPI

timezone = TIME_ZONE


__all__ = [
    'AINotificationRulesScanner',
]


class AINotificationRulesScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._prepare_logfile()
        self._db_session_class = init_db_session_class(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'ai_notification_rule_scanner.log')

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start ai notification rules scanner: it is not enabled!')
            return

        logging.info('Start ai notification rules scanner')

        AINotificationRulesScannerTimer(self._logfile, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def scan_ai_notification_rules(db_session):
    sql = f'''
            SELECT `id`, `assistant_uuid`, `detail` FROM ai_notification_rules
        '''
    rules = db_session.execute(text(sql))

    for rule in rules:
        try:
            trigger_ai_notification_rule(rule)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')


class AINotificationRulesScannerTimer(Thread):

    def __init__(self, logfile, db_session_class):
        super(AINotificationRulesScannerTimer, self).__init__()
        self._logfile = logfile
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan ai notification rules...')

            db_session = self.db_session_class()
            try:
                scan_ai_notification_rules(db_session)
            except Exception as e:
                logging.exception('error when scanning ai notification rules: %s', e)
            finally:
                db_session.close()

        sched.start()


def trigger_ai_notification_rule(rule):
    assistant_uuid = rule[1]
    detail = rule[2]

    detail = json.loads(detail)
    notify_hour = detail.get('users', 0)
    users = detail.get('users', [])

    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)

    cur_datetime = datetime.now()
    cur_hour = int(cur_datetime.hour)
    if notify_hour and int(notify_hour) != cur_hour:
        return

    dtable_web_api.add_issues_notification(users, assistant_uuid)
