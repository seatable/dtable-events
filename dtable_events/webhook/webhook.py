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


class Webhooker(object):
    """
    There are a few steps in this program:
    1. subscribe events from redis.
    2. query webhooks and generate jobs, then put them to queue.
    3. trigger jobs one by one.
    """
    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self._subscriber = self._redis_client.get_subscriber('table-events')
        self.job_queue = Queue()

    def start(self):
        logger.info('Starting handle webhook jobs...')
        tds = [Thread(target=self.add_jobs)]
        tds.extend([Thread(target=self.trigger_jobs, name='trigger_%s' % i) for i in range(2)])
        [td.start() for td in tds]

    def add_jobs(self):
        """all events from redis are kind of update so far"""
        while True:
            try:
                for message in self._subscriber.listen():
                    if message['type'] != 'message':
                        continue
                    try:
                        data = json.loads(message['data'])
                    except Exception as e:
                        logger.error('parse message error: %s' % e)
                        continue
                    session = self._db_session_class()
                    try:
                        event = {'data': data, 'event': 'update'}
                        dtable_uuid = data.get('dtable_uuid')
                        hooks = session.query(Webhooks).filter(Webhooks.dtable_uuid == dtable_uuid).all()
                        for hook in hooks:
                            request_body = hook.gen_request_body(event)
                            request_headers = hook.gen_request_headers(request_body)
                            job = {'webhook_id': hook.id, 'created_at': datetime.now(), 'status': PENDING,
                                   'url': hook.url, 'request_headers': request_headers, 'request_body': request_body}
                            self.job_queue.put(job)
                    except Exception as e:
                        logger.error('add jobs error: %s' % e)
                    finally:
                        session.close()
            except Exception as e:
                logger.error('webhook sub from redis error: %s', e)
                self._subscriber = self._redis_client.get_subscriber('table-events')

    def trigger_jobs(self):
        while True:
            try:
                job = self.job_queue.get()
                session = self._db_session_class()
                try:
                    body = job.get('request_body')
                    headers = job.get('request_headers')
                    response = requests.post(job['url'], json=body, headers=headers, timeout=30)
                except Exception as e:
                    logger.error('request error: %s', e)
                    webhook_job = WebhookJobs(job['webhook_id'], job['created_at'], datetime.now(), FAILURE,
                                              job['url'], job['request_headers'], job['request_body'], None, None)
                    session.add(webhook_job)
                    session.commit()
                else:
                    if 200 <= response.status_code < 300:
                        continue
                    else:
                        webhook_job = WebhookJobs(
                            job['webhook_id'], job['created_at'], datetime.now(), FAILURE, job['url'],
                            job['request_headers'], job['request_body'], response.status_code, response.text)
                        session.add(webhook_job)
                        session.commit()
                finally:
                    session.close()
            except Exception as e:
                logger.error('trigger job error: %s' % e)
