import json
import os
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from threading import Thread, Event, current_thread

from dtable_events.app.event_redis import RedisClient
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.auto_rules_utils import scan_triggered_automation_rules
from dtable_events.automations.auto_rules_stats_helper import auto_rules_stats_helper
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_dtable_owner_org_id
from dtable_events.utils.utils_metric import publish_metric, INSTANT_AUTOMATION_RULES_QUEUE_METRIC_HELP, \
    INSTANT_AUTOMATION_RULES_TRIGGERED_COUNT_HELP


class RateLimiter:

    def __init__(self, window_secs = 60 * 5, percent=25):
        self.window_secs = window_secs
        self.percent = percent
        self.window_start = time.time()
        self.counters = {}

    def get_key(self, owner, org_id):
        if org_id and org_id != -1:
            return org_id
        else:
            if '@seafile_group' in owner:
                return None
            return owner

    def is_allowed(self, owner, org_id):
        limit_key = self.get_key(owner, org_id)
        if not limit_key:
            return True
        now_time = time.time()
        time_interval = now_time - self.window_start
        if time_interval > self.window_secs:
            self.window_start = now_time
            return True
        total_time = self.counters.get(limit_key, 0)
        if total_time / self.window_secs > self.percent / 100:
            return False
        return True

    def record_time(self, owner, org_id, run_time):
        if time.time() - self.window_start > self.window_secs:
            self.counters = {}
        limit_key = self.get_key(owner, org_id)
        self.counters[limit_key] = self.counters.get(limit_key, 0) + run_time


class AutomationRuleHandler(Thread):
    def __init__(self, config):
        Thread.__init__(self)
        self._enabled = True
        self._finished = Event()
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config, socket_timeout=10)

        self.per_update_auto_rule_workers = 3
        self.queue = Queue()
        self.time_queue = Queue()

        self.log_none_message_timeout = 10 * 60

        self.rate_limiter = RateLimiter()

        self._parse_config(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """

        try:
            per_update_auto_rule_workers = int(os.environ.get('AUTOMATION_PER_UPDATE_WORKERS', '3'))
        except:
            per_update_auto_rule_workers = 3

        try:
            rate_limit_window_secs = int(os.environ.get('AUTOMATION_RATE_LIMIT_WINDOW_SECS', '300'))
        except:
            rate_limit_window_secs = 300

        try:
            rate_limit_percent = int(os.environ.get('AUTOMATION_RATE_LIMIT_PERCENT', '25'))
        except:
            rate_limit_percent = 25

        self.rate_limiter.window_secs = rate_limit_window_secs
        self.rate_limiter.percent = rate_limit_percent

        self.per_update_auto_rule_workers = per_update_auto_rule_workers

        self._enabled = os.environ.get('AUTOMATION_ENABLED', 'true').lower() == 'true'

    def is_enabled(self):
        return self._enabled

    def scan(self):
        while True:
            event = self.queue.get()
            publish_metric(self.queue.qsize(), 'realtime_automation_queue_size', INSTANT_AUTOMATION_RULES_QUEUE_METRIC_HELP)
            auto_rule_logger.info("Start to trigger rule %s in thread %s", event, current_thread().name)
            session = self._db_session_class()
            start_time = time.time()
            try:
                scan_triggered_automation_rules(event, session)
            except Exception as e:
                auto_rule_logger.exception('Handle automation rule with data %s failed: %s', event, e)
            finally:
                session.close()
                end_time = time.time()
            self.time_queue.put({'event': event, 'run_time': end_time - start_time})

    def start_workers(self):
        executor = ThreadPoolExecutor(max_workers=self.per_update_auto_rule_workers, thread_name_prefix='instant-auto-rules')
        for index in range(self.per_update_auto_rule_workers):
            executor.submit(self.scan)
        Thread(target=self.record_time_worker, daemon=True, name='record-time-worker').start()

    def record_time_worker(self):
        while True:
            time_info = self.time_queue.get()
            self.rate_limiter.record_time(time_info['event']['owner'], time_info['event']['org_id'], time_info['run_time'])

    def run(self):
        if not self.is_enabled():
            auto_rule_logger.info('Can not start handle automation rules: it is not enabled!')
            return
        auto_rule_logger.info('Starting handle automation rules...')
        subscriber = self._redis_client.get_subscriber('automation-rule-triggered')

        self.start_workers()

        trigger_count = 0
        last_message_time = datetime.now()

        publish_metric(self.queue.qsize(), 'realtime_automation_queue_size', INSTANT_AUTOMATION_RULES_QUEUE_METRIC_HELP)
        publish_metric(trigger_count, 'realtime_automation_triggered_count', INSTANT_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)

        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message is not None:
                    event = json.loads(message['data'])

                    db_session = self._db_session_class()
                    try:
                        dtable_uuid = event.get('dtable_uuid')
                        owner_info = get_dtable_owner_org_id(dtable_uuid, db_session)
                        if not self.rate_limiter.is_allowed(owner_info['owner'], owner_info['org_id']):
                            auto_rule_logger.info(f"owner {owner_info['owner']} org {owner_info['org_id']} rate limit exceed event {event} will not trigger")
                            continue
                        if auto_rules_stats_helper.is_exceed(db_session, owner_info['owner'], owner_info['org_id']):
                            auto_rule_logger.info(f"owner {owner_info['owner']} org {owner_info['org_id']} trigger count limit exceed {event} will not trigger")
                            continue
                        event.update(owner_info)
                    except Exception as e:
                        auto_rule_logger.exception(e)
                        continue
                    finally:
                        db_session.close()

                    self.queue.put(event)
                    publish_metric(self.queue.qsize(), 'realtime_automation_queue_size', INSTANT_AUTOMATION_RULES_QUEUE_METRIC_HELP)
                    auto_rule_logger.info(f"subscribe event {event}")

                    trigger_count += 1
                    publish_metric(trigger_count, 'realtime_automation_triggered_count', INSTANT_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)

                    last_message_time = datetime.now()
                else:
                    if (datetime.now() - last_message_time).seconds >= self.log_none_message_timeout:
                        auto_rule_logger.info(f'No message for {self.log_none_message_timeout}s...')
                        last_message_time = datetime.now()
                    time.sleep(0.5)
            except Exception as e:
                auto_rule_logger.exception('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
                last_message_time = datetime.now()
