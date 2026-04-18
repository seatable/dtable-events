# -*- coding: utf-8 -*-

import logging
from dataclasses import dataclass
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from dtable_events.app.config import ENABLE_OPERATION_LOG_DB, CLEAN_DB_ENABLED, CLEAN_DB_KEEP_DTABLE_SNAPSHOT_DAYS, \
    CLEAN_DB_KEEP_ACTIVITIES_DAYS, CLEAN_DB_KEEP_OPERATION_LOG_DAYS, CLEAN_DB_KEEP_DELETE_OPERATION_LOG_DAYS, \
    CLEAN_DB_KEEP_DTABLE_DB_OP_LOG_DAYS, CLEAN_DB_KEEP_NOTIFICATIONS_USERNOTIFICATION_DAYS, \
    CLEAN_DB_KEEP_DTABLE_NOTIFICATIONS_DAYS, CLEAN_DB_KEEP_SESSION_LOG_DAYS, CLEAN_DB_KEEP_AUTO_RULES_TASK_LOG_DAYS, \
    CLEAN_DB_KEEP_USER_ACTIVITY_STATISTICS_DAYS, CLEAN_DB_KEEP_DTABLE_APP_PAGES_OPERATION_LOG_DAYS
from dtable_events.db import init_db_session_class

__all__ = [
    'CleanDBRecordsWorker',
]


class CleanDBRecordsWorker(object):
    def __init__(self):
        self._enabled = False
        self._db_session_class = init_db_session_class()

        self._parse_config()

    def _parse_config(self):

        self._enabled = CLEAN_DB_ENABLED

        self._retention_config = RetentionConfig(
            dtable_snapshot=CLEAN_DB_KEEP_DTABLE_SNAPSHOT_DAYS,
            activities=CLEAN_DB_KEEP_ACTIVITIES_DAYS,
            operation_log=CLEAN_DB_KEEP_OPERATION_LOG_DAYS,
            delete_operation_log=CLEAN_DB_KEEP_DELETE_OPERATION_LOG_DAYS,
            dtable_db_op_log=CLEAN_DB_KEEP_DTABLE_DB_OP_LOG_DAYS,
            notifications_usernotification=CLEAN_DB_KEEP_NOTIFICATIONS_USERNOTIFICATION_DAYS,
            dtable_notifications=CLEAN_DB_KEEP_DTABLE_NOTIFICATIONS_DAYS,
            session_log=CLEAN_DB_KEEP_SESSION_LOG_DAYS,
            auto_rules_task_log=CLEAN_DB_KEEP_AUTO_RULES_TASK_LOG_DAYS,
            user_activity_statistics=CLEAN_DB_KEEP_USER_ACTIVITY_STATISTICS_DAYS,
            dtable_app_pages_operation_log=CLEAN_DB_KEEP_DTABLE_APP_PAGES_OPERATION_LOG_DAYS,
        )

    def start(self):
        logging.info('Start clean db records worker')
        if not self.is_enabled():
            logging.warning('Can not clean db records: it is not enabled!')
            return

        logging.info('Using the following retention config: %s', self._retention_config)

        CleanDBRecordsTask(self._db_session_class, self._retention_config).start()

    def is_enabled(self):
        return self._enabled


@dataclass
class RetentionConfig:
    """All retention times are in days"""
    dtable_snapshot: int = 365
    activities: int = 30
    operation_log: int = 14
    delete_operation_log: int = 30
    dtable_db_op_log: int = 14
    notifications_usernotification: int = 30
    dtable_notifications: int = 30
    session_log: int = 30
    auto_rules_task_log: int = 30
    # Disabled by default
    user_activity_statistics: int = 0
    dtable_app_pages_operation_log: int = 14


class CleanDBRecordsTask(Thread):
    def __init__(self, db_session_class, retention_config: RetentionConfig):
        super(CleanDBRecordsTask, self).__init__()
        self.db_session_class = db_session_class
        self.retention_config = retention_config

    def run(self):
        schedule = BlockingScheduler()

        @schedule.scheduled_job('cron', day_of_week='*', hour='0', minute='30', misfire_grace_time=600)
        def timed_job():
            logging.info('Start cleaning database...')

            session = self.db_session_class()

            try:
                clean_snapshots(session, self.retention_config.dtable_snapshot)
                clean_activities(session, self.retention_config.activities)
                if not ENABLE_OPERATION_LOG_DB:
                    clean_operation_log(session, self.retention_config.operation_log)
                    clean_dtable_db_op_log(session, self.retention_config.dtable_db_op_log)
                clean_delete_operation_log(session, self.retention_config.delete_operation_log)
                clean_notifications(session, self.retention_config.dtable_notifications)
                clean_user_notifications(session, self.retention_config.notifications_usernotification)
                clean_sessions(session, self.retention_config.session_log)
                clean_django_sessions(session)
                clean_auto_rules_task_log(session, self.retention_config.auto_rules_task_log)
                clean_user_activity_statistics(session, self.retention_config.user_activity_statistics)
                clean_dtable_app_pages_operation_log(session, self.retention_config.dtable_app_pages_operation_log)
            except:
                logging.exception('Could not clean database')
            finally:
                session.close()

        schedule.start()


def clean_snapshots(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "dtable_snapshot" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "dtable_snapshot" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `dtable_snapshot` WHERE `ctime` < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL :days DAY))*1000'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "dtable_snapshot"', result.rowcount)


def clean_activities(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "activities" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "activities" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `activities` WHERE `op_time` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "activities"', result.rowcount)


def clean_operation_log(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "operation_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "operation_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `operation_log` WHERE `op_time` < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL :days DAY))*1000'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "operation_log"', result.rowcount)


def clean_delete_operation_log(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "delete_operation_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "delete_operation_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `delete_operation_log` WHERE `op_time` < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL :days DAY))*1000'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "delete_operation_log"', result.rowcount)


def clean_dtable_db_op_log(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "dtable_db_op_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "dtable_db_op_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `dtable_db_op_log` WHERE `op_time` < UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL :days DAY))*1000'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "dtable_db_op_log"', result.rowcount)


def clean_user_notifications(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "notifications_usernotification" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "notifications_usernotification" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `notifications_usernotification` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "notifications_usernotification"', result.rowcount)


def clean_notifications(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "dtable_notifications" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "dtable_notifications" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `dtable_notifications` WHERE `created_at` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "dtable_notifications"', result.rowcount)


def clean_sessions(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "session_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "session_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `session_log` WHERE `op_time` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "session_log"', result.rowcount)

def clean_django_sessions(session):
    logging.info('Cleaning expired entries from "django_session" table')

    sql = 'DELETE FROM `django_session` WHERE `expire_date` < CURRENT_TIMESTAMP()'
    result = session.execute(text(sql))
    session.commit()

    logging.info('Removed %d entries from "django_session"', result.rowcount)

def clean_auto_rules_task_log(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "auto_rules_task_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "auto_rules_task_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `auto_rules_task_log` WHERE `trigger_time` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "auto_rules_task_log"', result.rowcount)

def clean_user_activity_statistics(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "user_activity_statistics" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "user_activity_statistics" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `user_activity_statistics` WHERE `timestamp` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "user_activity_statistics"', result.rowcount)

def clean_dtable_app_pages_operation_log(session, keep_days: int):
    if keep_days <= 0:
        logging.info('Skipping "dtable_app_pages_operation_log" since retention time is set to %d', keep_days)
        return

    logging.info('Cleaning "dtable_app_pages_operation_log" table (older than %d days)', keep_days)

    sql = 'DELETE FROM `dtable_app_pages_operation_log` WHERE `updated_at` < DATE_SUB(NOW(), INTERVAL :days DAY)'
    result = session.execute(text(sql), {'days': keep_days})
    session.commit()

    logging.info('Removed %d entries from "dtable_app_pages_operation_log"', result.rowcount)
