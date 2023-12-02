# -*- coding: utf-8 -*-
from sqlalchemy.orm import mapped_column
from sqlalchemy import Integer, String, DateTime, Text, Index

from dtable_events.db import Base


class Activities(Base):
    __tablename__ = 'activities'

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    dtable_uuid = mapped_column(String(length=36), nullable=False, index=True)
    row_id = mapped_column(String(length=36), nullable=False, index=True)
    row_count = mapped_column(Integer, nullable=False, default=1)
    op_user = mapped_column(String(length=255), nullable=False)
    op_type = mapped_column(String(length=128), nullable=False)
    op_time = mapped_column(DateTime, nullable=False, index=True)
    detail = mapped_column(Text, nullable=False)
    op_app = mapped_column(String(length=255))

    __table_args__ = (Index('ix_activities_op_time_dtable_uuid', 'op_time', 'dtable_uuid'),)

    def __init__(self, dtable_uuid, row_id, row_count, op_user, op_type, op_time, detail, op_app):
        super().__init__()
        self.dtable_uuid = dtable_uuid
        self.row_id = row_id
        self.row_count = row_count
        self.op_user = op_user
        self.op_type = op_type
        self.op_time = op_time
        self.detail = detail
        self.op_app = op_app
