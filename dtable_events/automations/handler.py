import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Thread, Event, current_thread

from dtable_events.app.event_redis import RedisClient
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.auto_rules_utils import scan_triggered_automation_rules
from dtable_events.celery_app.tasks.automation_rules import trigger_automation_rule
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env


class AutomationRuleHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._enabled = True
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)

        self.per_update_auto_rule_workers = 3
        self.queue = Queue()

        self._parse_config(config)

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
            auto_rule_logger.error('parse section: %s key: %s error: %s', section_name, per_update_auto_rule_workers, e)
            per_update_auto_rule_workers = 3

        self.per_update_auto_rule_workers = per_update_auto_rule_workers

    def is_enabled(self):
        return self._enabled

    def scan(self):
        while True:
            event = self.queue.get()
            auto_rule_logger.info("Start to trigger rule %s in thread %s", event, current_thread().name)
            session = self._db_session_class()
            try:
                scan_triggered_automation_rules(event, session)
            except Exception as e:
                auto_rule_logger.exception('Handle automation rule with data %s failed: %s', event, e)
            finally:
                session.close()

    def start_threads(self):
        executor = ThreadPoolExecutor(max_workers=self.per_update_auto_rule_workers, thread_name_prefix='instant-auto-rules')
        for index in range(self.per_update_auto_rule_workers):
            executor.submit(self.scan)

    def run(self):
        if not self.is_enabled():
            auto_rule_logger.info('Can not start handle automation rules: it is not enabled!')
            return
        auto_rule_logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')

        self.start_threads()

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    # self.queue.put(event)
                    # auto_rule_logger.info(f"subscribe event {event}")
                    trigger_automation_rule.delay(event)
                else:
                    time.sleep(0.5)
            except Exception as e:
                auto_rule_logger.exception('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
