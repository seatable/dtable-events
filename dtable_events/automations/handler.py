import json
import logging
import time
from collections import defaultdict
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
        self.thread_queues = []
        self.thread_uuids = []
        self.thread_status = []
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

    def scan(self, index):
        thread_queue = self.thread_queues[index]
        while True:
            event = thread_queue.get()
            with self.processing_lock:
                self.thread_status[index] = 'processing'
            logger.info("Start to trigger rule %s in thread %s", event, current_thread().name)
            session = self._db_session_class()
            try:
                scan_triggered_automation_rules(event, session)
            except Exception as e:
                logger.exception('Handle automation rule with data %s failed: %s', event, e)
            finally:
                session.close()
                with self.processing_lock:
                    self.thread_status[index] = 'idle'

    def schedule_events(self):
        while True:
            event = self.queue.get()
            logger.debug(f"start to scheduler event: {event}")
            event_dtable_uuid = event.get('dtable_uuid')
            existing_worker_index = None
            idle_worker_index = None
            while not (existing_worker_index is not None or idle_worker_index is not None):
                with self.processing_lock:
                    for index in range(self.per_update_auto_rule_workers):
                        if self.thread_queues[index].qsize() == 0 and self.thread_status[index] == 'idle':
                            idle_worker_index = index
                        if self.thread_uuids[index] == event_dtable_uuid:
                            existing_worker_index = index
                    if existing_worker_index is not None:
                        self.thread_queues[index].put(event)
                        logger.debug(f"schedule event {event} in index {index}, uuid matched")
                    elif idle_worker_index is not None:
                        self.thread_queues[index].put(event)
                        self.thread_uuids[index] = event_dtable_uuid
                        logger.debug(f"schedule event {event} in index {index}, idle worker")
                if not (existing_worker_index is not None or idle_worker_index is not None):
                    time.sleep(0.1)

    def start_threads(self):
        executor = ThreadPoolExecutor(max_workers=self.per_update_auto_rule_workers, thread_name_prefix='scan-auto-rules')
        for index in range(self.per_update_auto_rule_workers):
            executor.submit(self.scan, index)
        Thread(target=self.schedule_events).start()

    def run(self):
        if not self.is_enabled():
            logger.info('Can not start handle automation rules: it is not enabled!')
            return
        logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')

        self.thread_queues = [Queue() for _ in range(self.per_update_auto_rule_workers)]
        self.thread_uuids = [None for _ in range(self.per_update_auto_rule_workers)]
        self.thread_status = ['idle' for _ in range(self.per_update_auto_rule_workers)]

        self.start_threads()

        while not self._finished.is_set():
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
