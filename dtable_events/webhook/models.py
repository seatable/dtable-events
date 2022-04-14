import json
import hmac
import logging

from sqlalchemy import Column, Integer, String, DateTime, Text, text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT

from dtable_events.db import Base

logger = logging.getLogger(__name__)

PENDING = 0
FAILURE = 3


class Webhooks(Base):
    """
    webhooks model
    """
    __tablename__ = 'webhooks'

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    dtable_uuid = Column(String(32), nullable=False, index=True)
    url = Column(String(2000), nullable=False)
    settings = Column(Text)
    creator = Column(String(255), nullable=False)
    created_at = Column(DateTime, server_default=text('current_timestamp(6)'))

    @property
    def hook_settings(self):
        try:
            hook_settings = json.loads(self.settings)
        except (Exception, ):
            return {}
        return hook_settings

    def is_event_trigger(self, event):
        hook_settings = self.hook_settings
        if not hook_settings:
            return False
        events = hook_settings.get('events', [])
        if event in events:
            return True

    def gen_request_body(self, event):
        """
        must return dict
        """
        if event.get('event') == 'update':
            return {'event': 'update', 'data': event.get('data')}
        return {}

    def gen_request_headers(self, request_body):
        """
        must return dict
        """
        hook_settings = self.hook_settings
        if not hook_settings:
            return {}
        secret = hook_settings.get('secret')
        if not secret:
            return {}

        msg = json.dumps(request_body)
        signature = 'sha256=' + hmac.new(
            secret.encode('utf8'), msg.encode('utf8'), digestmod='sha256').hexdigest()
        return {'X-SeaTable-Signature': signature}


class WebhookJobs(Base):
    """
    webhook_jobs model
    """
    __tablename__ = 'webhook_jobs'

    id = Column(INTEGER(unsigned=True), primary_key=True, autoincrement=True)
    webhook_id = Column(INTEGER(unsigned=True), index=True, nullable=False)
    created_at = Column(DateTime, server_default=text('current_timestamp(6)'))
    trigger_at = Column(DateTime)
    status = Column(TINYINT, default=0, index=True)
    url = Column(String(2000), nullable=False)
    request_headers = Column(Text)
    request_body = Column(Text)
    response_status = Column(Integer)
    response_body = Column(Text)

    def __init__(self, webhook_id, created_at, trigger_at, status, url, request_headers,
                 request_body, response_status, response_body):
        self.webhook_id = webhook_id
        self.created_at = created_at
        self.trigger_at = trigger_at
        self.status = status
        self.url = url
        self.request_headers = json.dumps(request_headers) if isinstance(request_headers, dict) else request_headers
        self.request_body = json.dumps(request_body) if isinstance(request_body, dict) else request_body
        self.response_status = response_status
        self.response_body = json.dumps(response_body) if isinstance(response_body, dict) else response_body
