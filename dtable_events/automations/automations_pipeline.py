import json
import os
import time
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from threading import Thread, Lock

from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import text

from dtable_events.app.config import DTABLE_WEB_SERVICE_URL
from dtable_events.app.event_redis import RedisClient
from dtable_events.app.log import auto_rule_logger
from dtable_events.automations.automations_stats_manager import AutomationsStatsManager
from dtable_events.ccnet.organization import get_org_admins
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_dtable_owner_org_id
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.utils.utils_metric import AUTOMATION_QUEUE_10_METRIC_HELP, AUTOMATION_QUEUE_20_METRIC_HELP, \
    AUTOMATION_QUEUE_30_METRIC_HELP, REALTIME_AUTOMATION_RULES_HEARTBEAT_HELP, \
    REALTIME_AUTOMATION_RULES_TRIGGERED_COUNT_HELP, SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP, publish_metric
from dtable_events.automations.entities import AutomationResult, AutomationTask, QUEUE_AUTOMATION_TASKS_10, QUEUE_AUTOMATION_TASKS_20, QUEUE_AUTOMATION_TASKS_30


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


class AutomationsPipeline:

    def __init__(self, config):
        self.workers = 5
        self._db_session_class = init_db_session_class(config)

        self._redis_client = RedisClient(config, socket_timeout=10)
        self.per_update_channel = 'automation-rule-triggered'

        self.results_queue_key = 'automation_results'

        self.rate_limiter = RateLimiter()

        self.automations_stats_manager = AutomationsStatsManager()

        self.log_none_message_timeout = 10 * 60

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
        try:
            self.workers = int(os.environ.get('AUTOMATION_WORKERS', self.workers))
        except:
            pass

        try:
            rate_limit_window_secs = int(os.environ.get('AUTOMATION_RATE_LIMIT_WINDOW_SECS', '300'))
            self.rate_limiter.window_secs = rate_limit_window_secs
        except:
            pass

        try:
            rate_limit_percent = float(os.environ.get('AUTOMATION_RATE_LIMIT_PERCENT', '0.25'))
            self.rate_limiter.percent = rate_limit_percent
        except:
            pass

    def publish_metrics(self):
        while True:
            publish_metric(self.realtime_trigger_count, 'realtime_automation_triggered_count', REALTIME_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)
            publish_metric(self.scheduled_trigger_count, 'scheduled_automation_triggered_count', SCHEDULED_AUTOMATION_RULES_TRIGGERED_COUNT_HELP)
            publish_metric(self._redis_client.llen(QUEUE_AUTOMATION_TASKS_10), f'{QUEUE_AUTOMATION_TASKS_10}_size', AUTOMATION_QUEUE_10_METRIC_HELP)
            publish_metric(self._redis_client.llen(QUEUE_AUTOMATION_TASKS_20), f'{QUEUE_AUTOMATION_TASKS_20}_size', AUTOMATION_QUEUE_20_METRIC_HELP)
            publish_metric(self._redis_client.llen(QUEUE_AUTOMATION_TASKS_30), f'{QUEUE_AUTOMATION_TASKS_30}_size', AUTOMATION_QUEUE_30_METRIC_HELP)
            publish_metric(self.realtime_automation_heartbeat, 'realtime_automation_heartbeat', REALTIME_AUTOMATION_RULES_HEARTBEAT_HELP)
            time.sleep(10)

    def get_automation_task(self, db_session, event_data):
        sql = "SELECT `trigger`, `actions`, `run_condition`, `dtable_uuid` FROM dtable_automation_rules WHERE id=:rule_id AND is_valid=1 AND is_pause=0"
        rule = db_session.execute(text(sql), {'rule_id': event_data['automation_rule_id']}).fetchone()
        if not rule:
            return None
        owner_info = get_dtable_owner_org_id(rule.dtable_uuid, db_session)
        return AutomationTask(
            rule_id=event_data['automation_rule_id'],
            run_condition=rule.run_condition,
            trigger=json.loads(rule.trigger),
            actions=json.loads(rule.actions),
            dtable_uuid=rule.dtable_uuid,
            org_id=owner_info['org_id'],
            owner=owner_info['owner'],
            data=event_data,
            with_test=False
        )

    def put_task(self, automation_task: AutomationTask):
        queue_key = automation_task.get_priority_queue()
        self._redis_client.lpush(queue_key, json.dumps(automation_task.to_dict()))

    def receive(self):
        auto_rule_logger.info(f"Start to receive automation event from redis, window seconds {self.rate_limiter.window_secs} limit percent {self.rate_limiter.percent}")
        subscriber = self._redis_client.get_subscriber(self.per_update_channel)
        last_message_time = datetime.now()
        while True:
            try:
                message = subscriber.get_message()
                self.realtime_automation_heartbeat = time.time()
                if message is not None:
                    event = json.loads(message['data'])
                    auto_rule_logger.info(f"subscribe event {event}")
                    last_message_time = datetime.now()

                    db_session = self._db_session_class()
                    try:
                        dtable_uuid = event.get('dtable_uuid')
                        owner_info = get_dtable_owner_org_id(dtable_uuid, db_session)
                        event.update(owner_info)
                        automation_task = self.get_automation_task(db_session, event)
                        if not automation_task:
                            continue
                        if not automation_task.can_do_actions():
                            auto_rule_logger.info(f"owner {owner_info['owner']} org {owner_info['org_id']} trigger run condition missed, {event} will not trigger")
                            continue
                        if not self.rate_limiter.is_allowed(owner_info['owner'], owner_info['org_id'], self.workers):
                            auto_rule_logger.info(f"owner {owner_info['owner']} org {owner_info['org_id']} rate limit exceed, event {event} will not trigger")
                            automation_task.append_warning({'type': 'exceed_system_resource_limit'})
                            automation_result = AutomationResult(
                                rule_id=automation_task.rule_id,
                                rule_name=automation_task.rule_name,
                                dtable_uuid=automation_task.dtable_uuid,
                                run_condition=automation_task.run_condition,
                                org_id=automation_task.org_id,
                                owner=automation_task.owner,
                                with_test=automation_task.with_test,
                                success=False,
                                is_exceed_system_resource_limit=True,
                                trigger_time=datetime.utcnow(),
                                warnings=automation_task.warnings
                            )
                            self.add_exceed_system_resource_limit_entity(automation_task.owner, automation_task.org_id)
                            self._redis_client.lpush(self.results_queue_key, json.dumps(automation_result.to_dict()))
                            continue
                        if self.automations_stats_manager.is_exceed(db_session, owner_info['owner'], owner_info['org_id']):
                            auto_rule_logger.info(f"owner {owner_info['owner']} org {owner_info['org_id']} trigger count limit exceed, {event} will not trigger")
                            continue
                        self.put_task(automation_task)
                        self.realtime_trigger_count += 1
                    except Exception as e:
                        auto_rule_logger.exception(e)
                    finally:
                        db_session.close()
                else:
                    if (datetime.now() - last_message_time).seconds >= self.log_none_message_timeout:
                        auto_rule_logger.info(f'No message for {self.log_none_message_timeout}s...')
                        last_message_time = datetime.now()
                    time.sleep(0.5)
            except Exception as e:
                auto_rule_logger.exception('Failed get automation rules message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber('automation-rule-triggered')
                last_message_time = datetime.now()

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
            rules = db_session.execute(text(sql), {
                'per_day_check_time': per_day_check_time,
                'per_week_check_time': per_week_check_time,
                'per_month_check_time': per_month_check_time
            })
        except Exception as e:
            auto_rule_logger.exception('query regular automation rules error: %s', e)
            db_session.close()
            return

        cached_exceed_keys_set = set()
        gen_exceed_key = lambda owner, org_id: org_id if org_id != -1 else owner

        try:
            for rule in rules:
                automation_task = AutomationTask(
                    rule_id=rule.id,
                    run_condition=rule.run_condition,
                    trigger=json.loads(rule.trigger),
                    actions=json.loads(rule.actions),
                    dtable_uuid=rule.dtable_uuid,
                    org_id=rule.org_id,
                    owner=rule.owner,
                    data=None,
                    with_test=False
                )
                if not automation_task.can_do_actions():
                    continue
                exceed_key = gen_exceed_key(rule.owner, rule.org_id)
                if exceed_key in cached_exceed_keys_set:
                    continue
                if isinstance(exceed_key, str) and '@seafile_group' in exceed_key:
                    self.put_task(automation_task)
                    self.scheduled_trigger_count += 1
                    continue
                if self.automations_stats_manager.is_exceed(db_session, rule.owner, rule.org_id):
                    cached_exceed_keys_set.add(exceed_key)
                    continue
                self.put_task(automation_task)
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
            auto_rule_logger.info('Starts to scan scheduled automation rules...')

            try:
                self.scan_rules()
            except Exception as e:
                auto_rule_logger.exception('error when scanning scheduled automation rules: %s', e)

        sched.start()

    def stats(self):
        auto_rule_logger.info("Start to stats thread")
        while True:
            result_info_str = self._redis_client.rpop(self.results_queue_key)
            if not result_info_str:
                time.sleep(0.2)
                continue
            result_info = json.loads(result_info_str)
            try:
                result_info['trigger_time'] = datetime.fromisoformat(result_info['trigger_time'])
                result_info['trigger_date'] = datetime.fromisoformat(result_info['trigger_date'])
                result = AutomationResult(**result_info)
            except Exception as e:
                auto_rule_logger.exception(f'failed to load result {result_info}')
                continue
            if result.with_test:
                self._redis_client.set(self.get_test_task_cache_key(result.task_id), 1, timeout=30)
                continue
            if result.run_condition == 'per_update' and not result.is_exceed_system_resource_limit:
                owner = result.owner
                org_id = result.org_id
                run_time = result.run_time
                self.rate_limiter.record_time(owner, org_id, run_time)
                auto_rule_logger.debug(f"owner {owner} org_id {org_id} usage percent {self.rate_limiter.get_percent(owner, org_id, self.workers)}")
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
            dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
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

    def get_test_task_cache_key(self, task_id):
        return f'TEST_AUTOMATION:{task_id}'

    def put_test_task(self, automation_rule_id, task_id):
        # self.put_task(automation_task)
        db_session = self._db_session_class()
        try:
            sql = "SELECT `run_condition`, `trigger`, `actions`, `dtable_uuid` FROM dtable_automation_rules WHERE id=:automation_rule_id"
            rule = db_session.execute(text(sql), {'automation_rule_id': automation_rule_id}).fetchone()
            if not rule:
                return
            owner_info = get_dtable_owner_org_id(rule.dtable_uuid, db_session)
            automation_task = AutomationTask(
                rule_id=automation_rule_id,
                run_condition=rule.run_condition,
                trigger=json.loads(rule.trigger),
                actions=json.loads(rule.actions),
                dtable_uuid=rule.dtable_uuid,
                org_id=owner_info['org_id'],
                owner=owner_info['owner'],
                data=None,
                with_test=True,
                task_id=task_id
            )
        except Exception as e:
            raise Exception(f"Test run rule {automation_rule_id} error {e}")
        finally:
            db_session.close()
        self.put_task(automation_task)
        start_at = time.time()
        while True:
            done = self._redis_client.get(self.get_test_task_cache_key(automation_task.task_id))
            if done:
                break
            if time.time() - start_at > 15 * 60:
                raise Exception(f'Wait test automation {automation_task.rule_id} task id {automation_task.task_id} timeout')
            time.sleep(1)

    def start(self):
        auto_rule_logger.info("Start automations pipeline")
        Thread(target=self.receive, daemon=True).start() # add normal action to redis queue
        Thread(target=self.scheduled_scan, daemon=True).start() # add cron action to redis queue
        Thread(target=self.stats, daemon=True).start() # update status
        Thread(target=self.publish_metrics, daemon=True).start() # update metrics
        Thread(target=self.send_exceed_system_resource_limit_notifications, daemon=True).start() # send notifications
