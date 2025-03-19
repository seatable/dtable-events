import json
import logging
import time
from queue import Queue
from threading import Thread, Event, current_thread

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

        self.per_update_auto_rule_workers = 3

        self.queues = []
        self.threads = []

        self._parse_config(config)
        self.init_workers()

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AUTOMATION'

        if not config.has_section(section_name):
            return

        key_per_update_auto_rule_workers = 'per_update_auto_rule_workers'
        per_update_auto_rule_workers = get_opt_from_conf_or_env(config, section_name, key_per_update_auto_rule_workers, default=3)
        try:
            per_update_auto_rule_workers = int(per_update_auto_rule_workers)
        except Exception as e:
            logger.error('parse section: %s key: %s error: %s', section_name, per_update_auto_rule_workers, e)
            per_update_auto_rule_workers = 3

        self.per_update_auto_rule_workers = per_update_auto_rule_workers

    def is_enabled(self):
        return self._enabled

    def scan(self, index):
        while True:
            event = self.queues[index].get()
            logger.info("Start to trigger rule %s in thread %s", event, current_thread().name)
            session = self._db_session_class()
            try:
                scan_triggered_automation_rules(event, session)
            except Exception as e:
                logger.exception('Handle automation rule with data %s failed: %s', event, e)
            finally:
                session.close()

    def init_workers(self):
        for i in range(self.per_update_auto_rule_workers):
            self.queues.append(Queue())
            t = Thread(target=self.scan, args=(i,), name=f'automation-rule-worker-{i}')
            self.threads.append(t)

    def run(self):
        logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')

        for t in self.threads:
            t.start()
            logger.info('thread %s start', t.name)

        while not self._finished.is_set() and self.is_enabled():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    dtable_uuid = event.get('dtable_uuid')
                    logger.debug('get per_update auto-rule message %s', event)
                    index = ord(dtable_uuid[0]) % self.per_update_auto_rule_workers
                    self.queues[index].put(event)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.exception('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
