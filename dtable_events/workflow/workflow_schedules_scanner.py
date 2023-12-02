import json
import logging
from datetime import datetime
from threading import Thread

from sqlalchemy import text
from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool
from dtable_events.utils.dtable_web_api import DTableWebAPI


class WorkflowSchedulesScanner:

    def __init__(self, config):
        self._enabled = True
        self._parse_config(config)
        self._db_session_class = init_db_session_class(config)

    def _parse_config(self, config):
        section_name = 'WORKFLOW-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'WORKFLOW SCANNER'
            if not config.has_section(section_name):
                return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self._enabled:
            logging.warning('Can not start workflow schedules scanner: it is not enabled!')
            return

        logging.info('Start dtable workflow schedules scanner')
        WorkflowSchedulesScannerTimer(self._db_session_class).start()


def do_notify_schedule(schedule_id, task_id, action):
    try:
        offset = action['offset']
        token = action['token']
        to_users = action['to_users']
        if not to_users or not isinstance(to_users, list):
            return
        detail = {
            'task_id': task_id,
            'token': token,
            'offset': offset
        }
        dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
        dtable_web_api.internal_add_notification(to_users, 'workflow_processing_expired', detail)
    except Exception as e:
        logging.exception(e)
        logging.error('schedule_id: %s task_id: %s action: %s send notifications error: %s', schedule_id, task_id, action, e)


def scan_workflow_schedules(db_session):
    sql = '''
    SELECT id, task_id, schedule_time, action, is_executed, created_at FROM dtable_workflow_task_schedules
    WHERE schedule_time <= :utc_now AND is_executed = 0
    '''
    schedules = db_session.execute(text(sql), {'utc_now': datetime.utcnow()})
    for item in schedules:
        schedule_id = item.id
        task_id = item.task_id
        action = item.action
        logging.debug('start to execute schedule: %s, task_id: %s, action: %s', schedule_id, task_id, action)
        try:
            action = json.loads(action)
        except:
            logging.error('schedule: %s action: %s invalid', schedule_id, action)
            continue
        if action.get('type') == 'notify':
            do_notify_schedule(schedule_id, task_id, action)
        try:
            db_session.execute(text('UPDATE dtable_workflow_task_schedules SET is_executed=1 WHERE id=:schedule_id'), {
                'schedule_id': schedule_id
            })
            db_session.commit()
        except Exception as e:
            logging.error('update workflow schedule executed id: %s error: %s', schedule_id, e)


class WorkflowSchedulesScannerTimer(Thread):

    def __init__(self, db_session_class):
        super(WorkflowSchedulesScannerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire per 15 mins
        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='0,15,30,45')
        def timed_job():
            logging.info('Starts to scan workflow schedules...')

            db_session = self.db_session_class()
            try:
                scan_workflow_schedules(db_session)
            except Exception as e:
                logging.exception(e)
                logging.error('scan workflow schedules error: %s', e)
            finally:
                db_session.close()

        sched.start()
