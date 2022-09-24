import json
import logging
import time
from threading import Thread, Event

from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.notification_rules.notification_rules_utils import scan_triggered_notification_rules

logger = logging.getLogger(__name__)


class NotificationRuleHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)

    def run(self):
        logger.info('Starting handle notification rules...')
        subscriber = self._redis_client.get_subscriber('notification-rule-triggered')
        
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    logger.info('notification rule handler subscribe one message')
                    event = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        scan_triggered_notification_rules(event, db_session=session)
                    except Exception as e:
                        logger.error('Handle notification rules failed: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get notification rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('notification-rule-triggered')
