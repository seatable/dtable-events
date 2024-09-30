# coding: utf-8
from sqlalchemy.orm import mapped_column
from sqlalchemy.sql.sqltypes import Integer, String, DateTime, Text, BigInteger

from dtable_events.db import Base


class FileAudit(Base):
    __tablename__ = 'FileAudit'

    eid = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    timestamp = mapped_column(DateTime, nullable=False, index=True)
    etype = mapped_column(String(length=128), nullable=False)
    user = mapped_column(String(length=255), nullable=False, index=True)
    ip = mapped_column(String(length=45), nullable=False)
    device = mapped_column(Text, nullable=False)
    org_id = mapped_column(Integer, nullable=False)
    dtable_uuid = mapped_column(String(length=36), nullable=False, index=True)
    file_path = mapped_column(Text, nullable=False)

    def __init__(self, timestamp, etype, user, ip, device,
                 org_id, dtable_uuid, file_path):
        super().__init__()
        self.timestamp = timestamp
        self.etype = etype
        self.user = user
        self.ip = ip
        self.device = device
        self.org_id = org_id
        self.dtable_uuid = dtable_uuid
        self.file_path = file_path

    def __str__(self):
        if self.org_id > 0:
            return "FileAudit<EventType = %s, User = %s, IP = %s, Device = %s, DtableUuid = %s, \
                    OrgID = %s, FilePath = %s>" % \
                    (self.etype, self.user, self.ip, self.device,
                     self.org_id, self.dtable_uuid, self.file_path)
        else:
            return "FileAudit<EventType = %s, User = %s, IP = %s, Device = %s, DtableUuid = %s, FilePath = %s>" % \
                    (self.etype, self.user, self.ip, self.device, self.dtable_uuid, self.file_path)
