import logging
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from dtable_events.big_data.auto_archive_utils import run_dtable_auto_archive_task
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool


__all__ = [
    'DTableAutoArchiveTaskScanner',
]


class DTableAutoArchiveTaskScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._parse_config(config)
        self._db_session_class = init_db_session_class(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AUTOARCHIVE-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'AUTOARCHIVE SCANNER'
            if not config.has_section(section_name):
                return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start big data auto archive scanner: it is not enabled!')
            return

        logging.info('Start big data auto archive scanner')

        DTableAutoArchiveTaskScannerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def scan_auto_archive_tasks(db_session):
    sql = '''
            SELECT `bdar`.`id`, `run_condition`, `table_id`, `view_id`, `last_run_time`, `dtable_uuid`, `details`, bdar.`creator` FROM dtable_auto_archive_task bdar
            JOIN dtables d ON bdar.dtable_uuid=d.uuid
            WHERE ((run_condition='per_day' AND (last_run_time<:per_day_check_time OR last_run_time IS NULL))
            OR (run_condition='per_week' AND (last_run_time<:per_week_check_time OR last_run_time IS NULL))
            OR (run_condition='per_month' AND (last_run_time<:per_month_check_time OR last_run_time IS NULL)))
            AND bdar.is_valid=1 AND d.deleted=0
        '''
    per_day_check_time = datetime.now() - timedelta(hours=23)
    per_week_check_time = datetime.now() - timedelta(days=6)
    per_month_check_time = datetime.now() - timedelta(days=29)

    tasks = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time,
        'per_month_check_time': per_month_check_time
    })

    for task in tasks:
        try:
            run_dtable_auto_archive_task(task, db_session)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check task failed. {task}, error: {e}')
        db_session.commit()


class DTableAutoArchiveTaskScannerTimer(Thread):

    def __init__(self, db_session_class):
        super(DTableAutoArchiveTaskScannerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to auto archive...')

            db_session = self.db_session_class()
            try:
                scan_auto_archive_tasks(db_session)
            except Exception as e:
                logging.exception('error when scanning big data auto archives: %s', e)
            finally:
                db_session.close()

        sched.start()
