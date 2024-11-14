# -*- coding: utf-8 -*-

import logging
from dataclasses import dataclass
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from dtable_events.db import init_db_session_class, is_enable_operation_log_db

__all__ = [
    'CleanDBRecordsWorker',
]


class CleanDBRecordsWorker(object):
    def __init__(self, config):
        self._enabled = False
        self._db_session_class = init_db_session_class(config)

        if is_enable_operation_log_db(config):
            self._operation_log_db_session_class = init_db_session_class(config, db='operation_log_db')
        else:
            self._operation_log_db_session_class = None

        try:
            self._parse_config(config)
        except:
            logging.exception('Could not parse config, using default retention config instead')
            # Use default configuration
            self._retention_config = RetentionConfig()

    def _parse_config(self, config):
        section_name = 'CLEAN DB'

        self._enabled = config.getboolean(section_name, 'enabled', fallback=False)

        # Read retention times from config file
        dtable_snapshot = config.getint(section_name, 'keep_dtable_snapshot_days', fallback=365)
        activities = config.getint(section_name, 'keep_activities_days', fallback=30)
        operation_log = config.getint(section_name, 'keep_operation_log_days', fallback=14)
        delete_operation_log = config.getint(section_name, 'keep_delete_operation_log_days', fallback=30)
        dtable_db_op_log = config.getint(section_name, 'keep_dtable_db_op_log_days', fallback=30)
        notifications_usernotification = config.getint(section_name, 'keep_notifications_usernotification_days', fallback=30)
        dtable_notifications = config.getint(section_name, 'keep_dtable_notifications_days', fallback=30)
        session_log = config.getint(section_name, 'keep_session_log_days', fallback=30)
        auto_rules_task_log = config.getint(section_name, 'keep_auto_rules_task_log_days', fallback=30)
        # Disabled by default
        user_activity_statistics = config.getint(section_name, 'keep_user_activity_statistics_days', fallback=0)

        self._retention_config = RetentionConfig(
            dtable_snapshot=dtable_snapshot,
            activities=activities,
            operation_log=operation_log,
            delete_operation_log=delete_operation_log,
            dtable_db_op_log=dtable_db_op_log,
            notifications_usernotification=notifications_usernotification,
            dtable_notifications=dtable_notifications,
            session_log=session_log,
            auto_rules_task_log=auto_rules_task_log,
            user_activity_statistics=user_activity_statistics,
        )

    def start(self):
        logging.info('Start clean db records worker')
        if not self.is_enabled():
            logging.warning('Can not clean db records: it is not enabled!')
            return

        logging.info('Using the following retention config: %s', self._retention_config)

        CleanDBRecordsTask(self._db_session_class, self._operation_log_db_session_class, self._retention_config).start()

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


class CleanDBRecordsTask(Thread):
    def __init__(self, db_session_class, operation_log_db_session_class, retention_config: RetentionConfig):
        super(CleanDBRecordsTask, self).__init__()
        self.db_session_class = db_session_class
        self.operation_log_db_session_class = operation_log_db_session_class
        self.retention_config = retention_config

    def run(self):
        schedule = BlockingScheduler()

        @schedule.scheduled_job('cron', day_of_week='*', hour='0', minute='30', misfire_grace_time=600)
        def timed_job():
            logging.info('Start cleaning database...')

            session = self.db_session_class()
            if self.operation_log_db_session_class:
                operation_log_db_session = self.operation_log_db_session_class()
            else:
                operation_log_db_session = None

            try:
                clean_snapshots(session, self.retention_config.dtable_snapshot)
                clean_activities(session, self.retention_config.activities)
                clean_operation_log(operation_log_db_session or session, self.retention_config.operation_log)
                clean_delete_operation_log(operation_log_db_session or session, self.retention_config.delete_operation_log)
                clean_dtable_db_op_log(operation_log_db_session or session, self.retention_config.dtable_db_op_log)
                clean_notifications(session, self.retention_config.dtable_notifications)
                clean_user_notifications(session, self.retention_config.notifications_usernotification)
                clean_sessions(session, self.retention_config.session_log)
                clean_django_sessions(session)
                clean_auto_rules_task_log(session, self.retention_config.auto_rules_task_log)
                clean_user_activity_statistics(session, self.retention_config.user_activity_statistics)
            except:
                logging.exception('Could not clean database')
            finally:
                session.close()
                if operation_log_db_session:
                    operation_log_db_session.close()

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
