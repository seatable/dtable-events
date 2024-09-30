import logging
import datetime

from .models import FileAudit

logger = logging.getLogger(__name__)


def save_file_audit_event(session, timestamp, etype, user, ip, device, org_id, dtable_uuid, file_path):
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()

    file_audit = FileAudit(timestamp, etype, user, ip, device, org_id, dtable_uuid, file_path)

    session.add(file_audit)
    session.commit()
