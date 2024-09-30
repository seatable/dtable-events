# coding: utf-8
import logging
import logging.handlers
import datetime

from dtable_events.events.db import save_file_audit_event


def FileAuditEventHandler(config, session, msg):
    elements = msg['content'].split('\t')
    if len(elements) != 7:
        logging.warning("got bad message: %s", elements)
        return

    logging.warning('FileAuditEventHandler elements:%s', elements)

    timestamp = datetime.datetime.utcfromtimestamp(msg['ctime'])
    msg_type = elements[0]
    user_name = elements[1]
    ip = elements[2]
    user_agent = elements[3]
    org_id = elements[4]
    dtable_uuid = elements[5]
    file_path = elements[6]
    if not file_path.startswith('/'):
        file_path = '/' + file_path

    save_file_audit_event(session, timestamp, msg_type, user_name, ip,
                          user_agent, org_id, dtable_uuid, file_path)


def register_handlers(handlers):
    handlers.add_handler('seahub.audit:file-download-web', FileAuditEventHandler)
    handlers.add_handler('seahub.audit:file-download-api', FileAuditEventHandler)
    handlers.add_handler('seahub.audit:file-download-share-link', FileAuditEventHandler)
