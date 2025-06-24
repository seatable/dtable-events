import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from threading import Thread, Event, current_thread, Lock

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

        self.queue = Queue()
        self.deferred_queue = Queue()
        self.processing_uuids_set = set()
        self.processing_lock = Lock()

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
            logger.error('parse section: %s key: %s error: %s', section_name, per_update_auto_rule_workers, e)
            per_update_auto_rule_workers = 3

        self.per_update_auto_rule_workers = per_update_auto_rule_workers

    def is_enabled(self):
        return self._enabled

    def scan(self):
        while True:
            event = self.queue.get()
            event_dtable_uuid = event.get('dtable_uuid')
            with self.processing_lock:
                if event_dtable_uuid in self.processing_uuids_set:
                    self.deferred_queue.put(event)
                    continue
                self.processing_uuids_set.add(event_dtable_uuid)
            logger.info("Start to trigger rule %s in thread %s", event, current_thread().name)
            session = self._db_session_class()
            try:
                scan_triggered_automation_rules(event, session)
            except Exception as e:
                logger.exception('Handle automation rule with data %s failed: %s', event, e)
            finally:
                session.close()
                with self.processing_lock:
                    self.processing_uuids_set.remove(event_dtable_uuid)

    def deferred_requeue_loop(self):
        while True:
            try:
                event = self.deferred_queue.get_nowait()
                logger.debug('get event %s from deferred queue', event)
                self.queue.put(event)
            except Empty:
                time.sleep(1)
            time.sleep(0.02)

    def start_threads(self):
        executor = ThreadPoolExecutor(max_workers=self.per_update_auto_rule_workers, thread_name_prefix='scan-auto-rules')
        for _ in range(self.per_update_auto_rule_workers):
            executor.submit(self.scan)
        defer_executor = ThreadPoolExecutor(max_workers=self.per_update_auto_rule_workers, thread_name_prefix='scan-auto-rules-deferred-requeue')
        defer_executor.submit(self.deferred_requeue_loop)

    def run(self):
        logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')

        self.start_threads()

        while not self._finished.is_set() and self.is_enabled():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])
                    self.queue.put(event)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.exception('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
