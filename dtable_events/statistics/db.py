# -*- coding: utf-8 -*-
import logging
from hashlib import md5
from datetime import datetime

from sqlalchemy import func, desc, text, select, insert

from dtable_events.statistics.models import UserActivityStatistics, EmailSendingLog

logger = logging.getLogger(__name__)


def save_user_activity_stat(session, msg):
    username = msg['username']
    timestamp = msg['timestamp']

    user_time_md5 = md5((username + timestamp).encode('utf-8')).hexdigest()
    msg['user_time_md5'] = user_time_md5

    cmd = "REPLACE INTO user_activity_statistics (user_time_md5, username, timestamp, org_id)" \
          "values(:user_time_md5, :username, :timestamp, :org_id)"

    session.execute(text(cmd), msg)
    session.commit()


def get_user_activity_stats_by_day(session, start, end, offset='+00:00'):
    start_str = start.strftime('%Y-%m-%d 00:00:00')
    end_str = end.strftime('%Y-%m-%d 23:59:59')
    start_at_0 = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
    end_at_23 = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')

    try:
        stmt = select(
            func.date(func.convert_tz(UserActivityStatistics.timestamp, '+00:00', offset)).label("timestamp"),
            func.count(UserActivityStatistics.user_time_md5).label("number")
        ).where(UserActivityStatistics.timestamp.between(
            func.convert_tz(start_at_0, offset, '+00:00'), func.convert_tz(end_at_23, offset, '+00:00')
        )).group_by(func.date(func.convert_tz(UserActivityStatistics.timestamp, '+00:00', offset))).\
            order_by("timestamp")
        rows = session.execute(stmt).all()
    except Exception as e:
        logger.error('Get user activity statistics failed: %s' % e)
        rows = list()

    res = list()
    for row in rows:
        res.append((datetime.strptime(str(row[0]), '%Y-%m-%d'), row[1]))
    return res


def get_daily_active_users(session, date_day, start, count):
    date_str = date_day.strftime('%Y-%m-%d 00:00:00')
    date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')

    try:
        count_stmt = select(func.count(UserActivityStatistics.id)).where(UserActivityStatistics.timestamp == date)
        stmt = select(UserActivityStatistics).where(UserActivityStatistics.timestamp == date).group_by(
            UserActivityStatistics.username).slice(start, start + count)
        total_count = session.scalar(count_stmt)
        active_users = session.scalars(stmt).all()
    except Exception as e:
        logger.error('Get daily active users failed: %s' % e)
        total_count = 0
        active_users = list()

    return active_users, total_count


def save_email_sending_records(session, username, host, success):
    timestamp = datetime.utcnow()

    new_log = EmailSendingLog(username, timestamp, host, success)
    session.add(new_log)
    session.commit()


def batch_save_email_sending_records(session, username, host, send_state_list):
    timestamp = datetime.utcnow()
    session.execute(
        insert(EmailSendingLog),
        [{"username": username, "timestamp": timestamp, "host": host, "success": send_state}
         for send_state in send_state_list]
    )
    session.commit()


def get_email_sending_logs(session, start, end):
    if start < 0:
        logger.error('start must be non-negative')
        raise RuntimeError('start must be non-negative')

    if end < start:
        logger.error('end must be more than start')
        raise RuntimeError('end must be more than start')

    try:
        count_stmt = select(func.count(EmailSendingLog.id))
        stmt = select(EmailSendingLog).order_by(desc(EmailSendingLog.timestamp)).slice(start, end)
        total_count = session.scalar(count_stmt)
        logs = session.scalars(stmt).all()
    except Exception as e:
        logger.error('Get email sending logs failed: %s' % e)
        total_count = 0
        logs = list()

    return logs, total_count
