# -*- coding: utf-8 -*-
from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, DateTime, Boolean

from dtable_events.db import Base


class UserActivityStatistics(Base):
    __tablename__ = 'user_activity_statistics'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_time_md5 = mapped_column(String(length=32), unique=True)
    username = mapped_column(String(length=255), nullable=False)
    timestamp = mapped_column(DateTime, nullable=False, index=True)
    org_id = mapped_column(Integer, nullable=False)

    def __init__(self, user_time_md5, username, timestamp, org_id):
        super().__init__()
        self.user_time_md5 = user_time_md5
        self.username = username
        self.timestamp = timestamp
        self.org_id = org_id


class EmailSendingLog(Base):
    __tablename__ = 'email_sending_log'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    username = mapped_column(String(length=255), nullable=False)
    timestamp = mapped_column(DateTime, nullable=False, index=True)
    host = mapped_column(String(length=255), nullable=False)
    success = mapped_column(Boolean, nullable=False, default=False)

    def __init__(self, username, timestamp, host, success):
        super().__init__()
        self.username = username
        self.timestamp = timestamp
        self.host = host
        self.success = success
