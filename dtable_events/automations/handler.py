import json
import logging
import time
from threading import Thread, Event

from dtable_events.app.config import IS_PRO_VERSION
from dtable_events.app.event_redis import RedisClient
from dtable_events.automations.auto_rules_utils import scan_triggered_automation_rules
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env

logger = logging.getLogger(__name__)


class AutomationRuleHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._enabled = True
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self.per_minute_trigger_limit = 50
        self._parse_config(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AUTOMATION'
        key_per_minute_trigger_limit = 'per_minute_trigger_limit'

        if not config.has_section(section_name):
            return

        per_minute_trigger_limit = get_opt_from_conf_or_env(config, section_name, key_per_minute_trigger_limit, default=50)
        try:
            per_minute_trigger_limit = int(per_minute_trigger_limit)
        except Exception as e:
            logger.error('parse section: %s key: %s error: %s', section_name, key_per_minute_trigger_limit, e)
            per_minute_trigger_limit = 50

        self.per_minute_trigger_limit = per_minute_trigger_limit

    def is_enabled(self):
        return self._enabled and IS_PRO_VERSION

    def run(self):
        logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
        
        while not self._finished.is_set() and self.is_enabled():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    session = self._db_session_class()
                    try:
                        scan_triggered_automation_rules(event, session, self.per_minute_trigger_limit)
                    except Exception as e:
                        logger.error('Handle automation rules failed: %s' % e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
