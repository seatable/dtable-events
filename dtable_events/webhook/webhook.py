import json
import logging
from datetime import datetime
from threading import Thread
from queue import Queue

import requests

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.webhook.models import Webhooks, WebhookJobs, PENDING, FAILURE

logger = logging.getLogger(__name__)


class WebhookJob(object):

    def __init__(self, webhoook_job):
        self.id = webhoook_job.id
        self.webhook_id = webhoook_job.webhook_id
        self.created_at = webhoook_job.created_at
        self.trigger_at = webhoook_job.trigger_at
        self.status = webhoook_job.status
        self.url = webhoook_job.url
        self.request_headers = webhoook_job.request_headers
        self.request_body = webhoook_job.request_body
        self.response_status = webhoook_job.response_status
        self.response_body = webhoook_job.response_body


class Webhooker(object):
    """
    Webhooker is used to trigger webhooks, generate webhook jobs.
    There are a few steps in this program:
    1. recover PENDING jobs from db.
    2. subscribe events from redis.
    3. query webhooks and generate jobs, then put them to job_queue.
    4. trigger jobs one by one.
    Steps above run in multi-threads to improve efficiency.
    """

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self._subscriber = self._redis_client.get_subscriber('table-events')
        self.job_queue = Queue()

    def start(self):
        logger.info('Starting handle webhook jobs...')
        self.recover_pending_jobs()
        tds = [Thread(target=self.add_jobs)]
        tds.extend([Thread(target=self.trigger_jobs, name='trigger_%s' % i) for i in range(2)])
        [td.start() for td in tds]

    def recover_pending_jobs(self):
        pending_jobs = list()
        session = self._db_session_class()
        try:
            pending_jobs = session.query(WebhookJobs).filter(WebhookJobs.status == PENDING).all()
        except Exception as e:
            logger.error('query pending webhook jobs failed: %s' % e)
        finally:
            session.close()
        for job in pending_jobs:
            webhook_job = WebhookJob(job)
            self.job_queue.put(webhook_job)

    def add_jobs(self):
        """all events from redis are kind of update so far"""
        while True:
            try:
                for message in self._subscriber.listen():
                    if message['type'] != 'message':
                        continue
                    try:
                        data = json.loads(message['data'])
                    except (Exception,):
                        continue
                    event = {'data': data, 'event': 'update'}
                    dtable_uuid = data.get('dtable_uuid')
                    session = self._db_session_class()
                    try:
                        hooks = session.query(Webhooks).filter(Webhooks.dtable_uuid == dtable_uuid).all()
                        # validate webhooks one by one and generate / put job_queue
                        for hook in hooks:
                            request_body = hook.gen_request_body(event)
                            request_headers = hook.gen_request_headers()
                            job = WebhookJobs(hook.id, request_body, hook.url, request_headers=request_headers)
                            session.add(job)
                            session.commit()
                            webhook_job = WebhookJob(job)
                            self.job_queue.put(webhook_job)
                    except Exception as e:
                        logger.error(e)
                    finally:
                        session.close()
            except Exception as e:
                logger.error('webhook sub from redis error: %s', e)
                self._subscriber = self._redis_client.get_subscriber('table-events')

    def trigger_jobs(self):
        while True:
            webhook_job = self.job_queue.get()
            session = self._db_session_class()
            try:
                body = json.loads(webhook_job.request_body) if webhook_job.request_body else None
                headers = json.loads(webhook_job.request_headers) if webhook_job.request_headers else None
                response = requests.post(webhook_job.url, json=body, headers=headers)
            except Exception as e:
                logger.error('request error: %s', e)
                job = session.query(WebhookJobs).filter(WebhookJobs.id == webhook_job.id)
                job.update({'trigger_at': datetime.now(), 'status': FAILURE})
                session.commit()
            else:
                # delete job after succeed
                if 200 <= response.status_code < 300:
                    session.query(WebhookJobs).filter(WebhookJobs.id == webhook_job.id).delete()
                    session.commit()
                else:
                    job = session.query(WebhookJobs).filter(WebhookJobs.id == webhook_job.id)
                    job.update({
                        'trigger_at': datetime.now(), 'status': FAILURE,
                        'response_status': response.status_code, 'response_body': response.text})
                    session.commit()
            finally:
                session.close()
