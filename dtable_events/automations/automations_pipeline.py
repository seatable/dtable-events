import json
import os
import time
from datetime import datetime, timedelta
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from threading import Thread, Lock

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from dtable_events.app.config import INNER_DTABLE_WEB_SERVICE_URL, AUTOMATION_RATE_LIMIT_PERCENT, \
    AUTOMATION_RATE_LIMIT_WINDOW_SECS, AUTOMATION_WORKERS
from dtable_events.app.event_redis import RedisClient
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.actions import AutomationRule, AutomationResult
from dtable_events.automations.automations_stats_manager import AutomationsStatsManager
from dtable_events.ccnet.organization import get_org_admins
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_dtable_owner_org_id
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.utils.utils_metric import AUTOMATION_RULES_QUEUE_METRIC_HELP, REALTIME_AUTOMATION_RULES_HEARTBEAT_HELP, \
    REALTIME_AUTOMATION_RULES_TRIGGERED_COUNT_HELP, SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP, publish_metric


class RateLimiter:

    def __init__(self, window_secs = 60 * 5, percent=0.25):
        self.window_secs = window_secs
        self.percent = percent
        self.window_start = time.time()
        self.counters = {}
        self.counters_lock = Lock()

    def get_key(self, owner, org_id):
        if org_id and org_id != -1:
            return org_id
        else:
            return owner

    def is_allowed(self, owner, org_id, workers):
        limit_key = self.get_key(owner, org_id)
        if isinstance(limit_key, str) and '@seafile_group' in limit_key:
            return True
        with self.counters_lock:
            if time.time() - self.window_start > self.window_secs:
                return True
            total_time = self.counters.get(limit_key, 0)
            auto_rule_logger.debug(f"owner {owner} org_id {org_id} usage percent {self.get_percent(owner, org_id, workers)}")
            if total_time / (self.window_secs * workers) > self.percent:
                return False
            return True

    def record_time(self, owner, org_id, run_time):
        with self.counters_lock:
            now_time = time.time()
            if now_time - self.window_start > self.window_secs:
                self.counters.clear()
                self.window_start = now_time
            limit_key = self.get_key(owner, org_id)
            self.counters[limit_key] = self.counters.get(limit_key, 0) + run_time

    def get_percent(self, owner, org_id, workers):
        limit_key = self.get_key(owner, org_id)
        return self.counters.get(limit_key, 0) / (self.window_secs * workers)


class AutomationsPipeline(object):

    def __init__(self):
        self.workers = 5
        self.automations_queue: Queue[AutomationRule] = Queue()
        self.results_queue: Queue[AutomationResult] = Queue()
        self._pubsub_no_message_timeout = 5 * 60

        self._db_session_class = init_db_session_class()

        self._redis_client = RedisClient(socket_connect_timeout=5, socket_timeout=10,
                                         health_check_interval=30, retry_on_timeout=True)
        self.per_update_channel = 'automation-rule-triggered'

        self.rate_limiter = RateLimiter()

        self.automations_stats_manager = AutomationsStatsManager()

        # metrics
        self.realtime_trigger_count = 0
        self.scheduled_trigger_count = 0
        self.realtime_automation_heartbeat = time.time()
        ## metric_times record the lastest publish times of metrics
        self.metric_times = {}

        self.parse_config()

        self.exceed_system_resource_limit_entities = None
        self.reset_exceed_system_resource_limit_entities()

    def reset_exceed_system_resource_limit_entities(self):
        self.exceed_system_resource_limit_entities = {'orgs_map': {}, 'owners_map': {}}

    def add_exceed_system_resource_limit_entity(self, owner, org_id):
        if org_id != -1:
            if org_id in self.exceed_system_resource_limit_entities['orgs_map']:
                self.exceed_system_resource_limit_entities['orgs_map'][org_id] += 1
            else:
                self.exceed_system_resource_limit_entities['orgs_map'][org_id] = 1
        else:
            if owner in self.exceed_system_resource_limit_entities['owners_map']:
                self.exceed_system_resource_limit_entities['owners_map'][owner] += 1
            else:
                self.exceed_system_resource_limit_entities['owners_map'][owner] = 1

    def parse_config(self):
        self.workers = AUTOMATION_WORKERS

        self.rate_limiter.window_secs = AUTOMATION_RATE_LIMIT_WINDOW_SECS

        self.rate_limiter.percent = AUTOMATION_RATE_LIMIT_PERCENT

    def publish_metrics(self):
        while True:
            publish_metric(self.realtime_trigger_count, 'realtime_automation_triggered_count', REALTIME_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)
            publish_metric(self.scheduled_trigger_count, 'scheduled_automation_triggered_count', SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)
            publish_metric(self.automations_queue.qsize(), 'automation_queue_size', AUTOMATION_RULES_QUEUE_METRIC_HELP)
            publish_metric(self.realtime_automation_heartbeat, 'realtime_automation_heartbeat', REALTIME_AUTOMATION_RULES_HEARTBEAT_HELP)
            time.sleep(10)

    def get_automation_rule(self, db_session, event_data):
        sql = "SELECT `trigger`, `actions`, `run_condition`, `dtable_uuid` FROM dtable_automation_rules WHERE id=:rule_id AND is_valid=1 AND is_pause=0"
        rule = db_session.execute(text(sql), {'rule_id': event_data['automation_rule_id']}).fetchone()
        if not rule:
            return None
        owner_info = get_dtable_owner_org_id(rule.dtable_uuid, db_session)
        options = {
            'rule_id': event_data['automation_rule_id'],
            'run_condition': rule.run_condition,
            'dtable_uuid': rule.dtable_uuid,
            'org_id': owner_info['org_id'],
            'owner': owner_info['owner']
        }
        return AutomationRule(event_data, rule.trigger, rule.actions, options)

    def receive(self):
        auto_rule_logger.info(
            "Start consuming automation events from Redis: window_secs=%s limit_percent=%s",
            self.rate_limiter.window_secs,
            self.rate_limiter.percent,
        )
        subscriber = self._redis_client.get_subscriber(self.per_update_channel)
        last_pubsub_message_time = time.time()
        while True:
            try:
                message = subscriber.get_message()
                self.realtime_automation_heartbeat = time.time()
                if message is not None:
                    if message.get('type') != 'message':
                        continue
                    last_pubsub_message_time = time.time()
                    event = json.loads(message['data'])
                    auto_rule_logger.info(
                        "Received automation event: rule_id=%s dtable_uuid=%s op_type=%s table_id=%s row_id=%s updated_column_keys=%s",
                        event.get('automation_rule_id'),
                        event.get('dtable_uuid'),
                        event.get('op_type'),
                        event.get('table_id'),
                        event.get('row_id'),
                        event.get('updated_column_keys'),
                    )

                    db_session = self._db_session_class()
                    try:
                        dtable_uuid = event.get('dtable_uuid')
                        owner_info = get_dtable_owner_org_id(dtable_uuid, db_session)
                        event.update(owner_info)
                        automation_rule = self.get_automation_rule(db_session, event)
                        if not automation_rule:
                            continue
                        if not self.rate_limiter.is_allowed(owner_info['owner'], owner_info['org_id'], self.workers):
                            auto_rule_logger.info(
                                "Skip automation event: rate limited owner=%s org_id=%s rule_id=%s",
                                owner_info['owner'],
                                owner_info['org_id'],
                                automation_rule.rule_id,
                            )
                            automation_rule.append_warning({'type': 'exceed_system_resource_limit'})
                            self.results_queue.put(AutomationResult(
                                rule_id=automation_rule.rule_id,
                                rule_name=automation_rule.rule_name,
                                dtable_uuid=automation_rule.dtable_uuid,
                                run_condition=automation_rule.run_condition,
                                org_id=automation_rule.org_id,
                                owner=automation_rule.owner,
                                with_test=False,
                                success=False,
                                is_exceed_system_resource_limit=True,
                                trigger_time=datetime.utcnow(),
                                warnings=automation_rule.warnings
                            ))
                            self.add_exceed_system_resource_limit_entity(automation_rule.owner, automation_rule.org_id)
                            continue
                        if self.automations_stats_manager.is_exceed(db_session, owner_info['owner'], owner_info['org_id']):
                            auto_rule_logger.info(
                                "Skip automation event: trigger quota exceeded owner=%s org_id=%s rule_id=%s",
                                owner_info['owner'],
                                owner_info['org_id'],
                                automation_rule.rule_id,
                            )
                            continue
                        if not automation_rule.can_do_actions():
                            auto_rule_logger.info(
                                "Skip automation event: trigger conditions not met rule_id=%s owner=%s org_id=%s",
                                automation_rule.rule_id,
                                owner_info['owner'],
                                owner_info['org_id'],
                            )
                            continue
                        self.automations_queue.put(automation_rule)
                        self.realtime_trigger_count += 1
                    except Exception as e:
                        auto_rule_logger.exception(e)
                    finally:
                        db_session.close()
                else:
                    if time.time() - last_pubsub_message_time >= self._pubsub_no_message_timeout:
                        auto_rule_logger.info('no automation message for %ss', self._pubsub_no_message_timeout)
                        subscriber = self._redis_client.refresh_subscriber(
                            subscriber, self.per_update_channel, 'no message timeout')
                        last_pubsub_message_time = time.time()
                        continue
                    time.sleep(0.5)
            except Exception as e:
                auto_rule_logger.error('Redis pubsub receive failed: %s', e)
                subscriber = self._redis_client.refresh_subscriber(subscriber, self.per_update_channel, str(e))
                last_pubsub_message_time = time.time()

    def worker(self):
        while True:
            automation = self.automations_queue.get()
            row_id = automation.data.get('row_id') if isinstance(automation.data, dict) else None
            updated_column_keys = automation.data.get('updated_column_keys') if isinstance(automation.data, dict) else None
            auto_rule_logger.info(
                'Automation started: rule_id=%s rule_name=%s dtable_uuid=%s run_condition=%s row_id=%s updated_column_keys=%s',
                automation.rule_id,
                automation.rule_name,
                automation.dtable_uuid,
                automation.run_condition,
                row_id,
                updated_column_keys,
            )
            db_session = self._db_session_class()
            try:
                start_time = time.time()
                result = automation.do_actions(db_session)
                run_time = time.time() - start_time
                auto_rule_logger.info(
                    'Automation finished: rule_id=%s success=%s run_time=%.3fs exceed_limit=%s warnings=%s',
                    automation.rule_id,
                    result.success if result else None,
                    run_time,
                    result.is_exceed_system_resource_limit if result else None,
                    len(result.warnings) if result else 0
                )
                if result:
                    result.run_time = run_time
                    self.results_queue.put(result)
            except Exception as e:
                auto_rule_logger.exception('Handle automation rule with data %s failed: %s', automation.data, e)
            finally:
                db_session.close()

    def start_workers(self):
        executor = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix='automations-pipeline-worker')
        for _ in range(self.workers):
            executor.submit(self.worker)
        auto_rule_logger.info(f"Started {self.workers} automation workers")

    def scan_rules(self):
        sql = '''
            SELECT `dar`.`id`, `run_condition`, `trigger`, `actions`, `dtable_uuid`, w.`owner`, w.`org_id` FROM dtable_automation_rules dar
            JOIN dtables d ON dar.dtable_uuid=d.uuid
            JOIN workspaces w ON d.workspace_id=w.id
            WHERE ((run_condition='per_day' AND (last_trigger_time<:per_day_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_week' AND (last_trigger_time<:per_week_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_month' AND (last_trigger_time<:per_month_check_time OR last_trigger_time IS NULL)))
            AND dar.is_valid=1 AND d.deleted=0 AND is_pause=0
        '''
        per_day_check_time = datetime.utcnow() - timedelta(hours=23)
        per_week_check_time = datetime.utcnow() - timedelta(days=6)
        per_month_check_time = datetime.utcnow() - timedelta(days=27)  # consider the least month-days 28 in February (the 2nd month) in common years
        db_session = self._db_session_class()
        try:
            auto_rule_logger.info('Scanning scheduled automation rules...')
            rules = db_session.execute(text(sql), {
                'per_day_check_time': per_day_check_time,
                'per_week_check_time': per_week_check_time,
                'per_month_check_time': per_month_check_time
            })
        except Exception as e:
            auto_rule_logger.exception('Failed to query scheduled automation rules: %s', e)
            db_session.close()
            return

        cached_exceed_keys_set = set()
        gen_exceed_key = lambda owner, org_id: org_id if org_id != -1 else owner

        try:
            for rule in rules:
                options = {
                    'rule_id': rule.id,
                    'run_condition': rule.run_condition,
                    'dtable_uuid': rule.dtable_uuid,
                    'org_id': rule.org_id,
                    'owner': rule.owner
                }
                automation = AutomationRule(None, rule.trigger, rule.actions, options)
                if not automation.can_do_actions():
                    continue
                exceed_key = gen_exceed_key(rule.owner, rule.org_id)
                if exceed_key in cached_exceed_keys_set:
                    continue
                if isinstance(exceed_key, str) and '@seafile_group' in exceed_key:
                    self.automations_queue.put(automation)
                    self.scheduled_trigger_count += 1
                    continue
                if self.automations_stats_manager.is_exceed(db_session, rule.owner, rule.org_id):
                    cached_exceed_keys_set.add(exceed_key)
                    continue
                self.automations_queue.put(automation)
                self.scheduled_trigger_count += 1
        except Exception as e:
            auto_rule_logger.exception(e)
        finally:
            db_session.close()

    def scheduled_scan(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*', misfire_grace_time=600)
        def timed_job():
            try:
                self.scan_rules()
            except Exception as e:
                auto_rule_logger.exception('Failed to scan scheduled automation rules: %s', e)

        sched.start()

    def stats(self):
        auto_rule_logger.info("Start stats thread")
        while True:
            result = self.results_queue.get()
            if result.run_condition == 'per_update' and not result.is_exceed_system_resource_limit:
                owner = result.owner
                org_id = result.org_id
                run_time = result.run_time
                self.rate_limiter.record_time(owner, org_id, run_time)
                auto_rule_logger.debug(
                    'Automation usage percent owner=%s org_id=%s percent=%s',
                    owner,
                    org_id,
                    self.rate_limiter.get_percent(owner, org_id, self.workers),
                )
            db_session = self._db_session_class()
            try:
                self.automations_stats_manager.update_stats(db_session, result)
            except Exception as e:
                auto_rule_logger.exception(e)
            finally:
                db_session.close()

    def send_exceed_system_resource_limit_notifications(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='*/10', misfire_grace_time=60)
        def timed_job():
            orgs_map = self.exceed_system_resource_limit_entities['orgs_map']
            db_session = self._db_session_class()
            dtable_web_api = DTableWebAPI(INNER_DTABLE_WEB_SERVICE_URL)
            try:
                for org_id, missing_count in orgs_map.items():
                    admins = get_org_admins(db_session, org_id)
                    if admins:
                        dtable_web_api.internal_add_notification(admins, 'auto_exceed_sys_res_limit', {'missing_count': missing_count})
                owners_map = self.exceed_system_resource_limit_entities['owners_map']
                for owner, missing_count in owners_map.items():
                    dtable_web_api.internal_add_notification([owner], 'auto_exceed_sys_res_limit', {'missing_count': missing_count})
            except Exception as e:
                auto_rule_logger.exception(e)
            finally:
                db_session.close()
            self.reset_exceed_system_resource_limit_entities()

        sched.start()

    def start(self):
        auto_rule_logger.info("Start automations pipeline")
        self.start_workers() # auto rules do action from automations_queue
        Thread(target=self.receive, daemon=True).start() # add normal action to automations_queue
        Thread(target=self.scheduled_scan, daemon=True).start() # add cron action to automations_queue
        Thread(target=self.stats, daemon=True).start() # update status
        Thread(target=self.publish_metrics, daemon=True).start() # update metrics
        Thread(target=self.send_exceed_system_resource_limit_notifications, daemon=True).start() # send notifications
