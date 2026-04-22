import json
import logging
import time
from threading import Thread, Event

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.notification_rules.notification_rules_utils import scan_triggered_notification_rules

logger = logging.getLogger(__name__)


class NotificationRuleHandler(Thread):
    def __init__(self):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class()
        self._redis_client = RedisClient(socket_connect_timeout=5, socket_timeout=5,
                                         health_check_interval=30, retry_on_timeout=True)
        self._pubsub_channel_name = 'notification-rule-triggered'
        self._pubsub_health_check_interval = 30
        self._pubsub_no_message_timeout = 5 * 60

    def run(self):
        logger.info('Starting handle notification rules...')
        subscriber = self._redis_client.get_subscriber(self._pubsub_channel_name)
        last_pubsub_message_time = time.time()
        last_pubsub_health_check_time = last_pubsub_message_time
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    if message.get('type') != 'message':
                        continue
                    event = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        scan_triggered_notification_rules(event, db_session=session)
                    except Exception as e:
                        logger.error('Handle notification rules failed: %s' % e)
                    finally:
                        session.close()
                else:
                    now = time.time()
                    if now - last_pubsub_health_check_time >= self._pubsub_health_check_interval:
                        last_pubsub_health_check_time = now
                        try:
                            subscriber.ping()
                        except Exception as e:
                            subscriber = self._redis_client.refresh_subscriber(
                                subscriber, self._pubsub_channel_name, 'health check failed: %s' % e)
                            last_pubsub_message_time = time.time()
                            last_pubsub_health_check_time = last_pubsub_message_time
                            continue
                    if (time.time() - last_pubsub_message_time) >= self._pubsub_no_message_timeout:
                        subscriber = self._redis_client.refresh_subscriber(
                            subscriber, self._pubsub_channel_name, 'no message timeout')
                        last_pubsub_message_time = time.time()
                        last_pubsub_health_check_time = last_pubsub_message_time
                        continue
                    time.sleep(0.5)
            except Exception as e:
                logger.error('redis pubsub receive error: %s', e)
                subscriber = self._redis_client.refresh_subscriber(subscriber, self._pubsub_channel_name, str(e))
                last_pubsub_message_time = time.time()
                last_pubsub_health_check_time = last_pubsub_message_time
