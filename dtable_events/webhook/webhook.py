import json
import logging
from datetime import datetime
from threading import Thread
from queue import Queue

import requests
from requests.exceptions import ReadTimeout

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.webhook.models import Webhooks, WebhookJobs, PENDING, FAILURE

logger = logging.getLogger(__name__)

WEBHOOK_ERROR_CACHE_PREFIX = 'webhook_error_'
WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT = 24 * 60 * 60
WEBHOOK_ALLOW_ERROR_TIMES = 5


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
                        hooks = session.query(Webhooks).filter(Webhooks.dtable_uuid == dtable_uuid, Webhooks.is_valid == 1).all()
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

    def invalidate_webhook(self, webhook_id, db_session):
        sql = "UPDATE webhooks SET is_valid=0 WHERE id=:webhook_id"
        try:
            db_session.execute(sql, {'webhook_id': webhook_id})
            db_session.commit()
        except Exception as e:
            logger.error('invalidate webhook: %s error: %s', webhook_id, e)

    def get_webhook_error_times(self, cache_key):
        webhook_error_times = self._redis_client.get(cache_key)
        if not webhook_error_times:
            webhook_error_times = 0

        return int(webhook_error_times)

    def save_webhook_job(self, session, job_params):
        webhook_job = WebhookJobs(*job_params)
        session.add(webhook_job)
        session.commit()

    def trigger_jobs(self):
        while True:
            try:
                job = self.job_queue.get()
                session = self._db_session_class()
                need_invalidate = False
                webhook_error_cache_key = WEBHOOK_ERROR_CACHE_PREFIX + str(job['webhook_id'])
                try:
                    body = job.get('request_body')
                    headers = job.get('request_headers')
                    response = requests.post(job['url'], json=body, headers=headers, timeout=30)
                except ReadTimeout:
                    logger.warning('request webhook url: %s timeout', job['url'])

                    job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE,
                                  job['url'], job['request_headers'], job['request_body'], None, None)
                    self.save_webhook_job(session, job_params)

                    webhook_error_times = self.get_webhook_error_times(webhook_error_cache_key) + 1
                    if webhook_error_times >= WEBHOOK_ALLOW_ERROR_TIMES:
                        need_invalidate = True
                    self._redis_client.set(webhook_error_cache_key,
                                           webhook_error_times,
                                           timeout=WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT
                                           )
                except Exception as e:
                    logger.warning('request webhook url: %s error: %s', job['url'], e)
                    need_invalidate = True

                    job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE,
                                  job['url'], job['request_headers'], job['request_body'], None, None)
                    self.save_webhook_job(session, job_params)
                else:
                    if 200 <= response.status_code < 300:
                        self._redis_client.delete(webhook_error_cache_key)
                        continue
                    else:
                        job_params = (job['webhook_id'], job['created_at'], datetime.now(), FAILURE, job['url'],
                                      job['request_headers'], job['request_body'], response.status_code, response.text)
                        self.save_webhook_job(session, job_params)

                        webhook_error_times = self.get_webhook_error_times(webhook_error_cache_key) + 1
                        if webhook_error_times >= WEBHOOK_ALLOW_ERROR_TIMES:
                            need_invalidate = True
                        self._redis_client.set(webhook_error_cache_key,
                                               webhook_error_times,
                                               timeout=WEBHOOK_ERROR_TIMES_CACHE_TIMEOUT
                                               )
                finally:
                    if need_invalidate:
                        self.invalidate_webhook(job['webhook_id'], session)
                        self._redis_client.delete(webhook_error_cache_key)
                    session.close()
            except Exception as e:
                logger.error('trigger job error: %s' % e)
