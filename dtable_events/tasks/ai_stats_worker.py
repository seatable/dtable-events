import json
import logging
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from threading import Thread, Lock

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil import relativedelta
from sqlalchemy import text

from dtable_events.app.config import AI_PRICES, BAIDU_OCR_TOKENS
from dtable_events.app.event_redis import RedisClient, redis_cache
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, uuid_str_to_36_chars

logger = logging.getLogger(__name__)


class AIStatsWorker:

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self.stats_lock = Lock()
        self.org_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        self.owner_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        self.channel = 'log_ai_model_usage'
        self.keep_months = 3
        self.owner_info_cache_timeout = 24 * 60 * 60
        self._parse_config(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AI STATS'
        key_enabled = 'enabled'

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def save_to_memory(self, usage_info, session):
        if not usage_info.get('model') or not usage_info.get('assistant_uuid'):
            return

        assistant_uuid = uuid_str_to_36_chars(usage_info.get('assistant_uuid'))
        model = usage_info['model']
        usage = usage_info.get('usage')

        if model not in AI_PRICES:
            logger.warning('model %s price not defined')
            return

        if 'prompt_tokens' in usage:
            usage['input_tokens'] = usage['prompt_tokens']
        if 'completion_tokens' in usage:
            usage['output_tokens'] = usage['completion_tokens']

        if not isinstance(usage.get('input_tokens'), int):
            usage['input_tokens'] = 0
        if not isinstance(usage.get('output_tokens'), int):
            usage['output_tokens'] = 0

        if model in BAIDU_OCR_TOKENS:
            usage['output_tokens'] = BAIDU_OCR_TOKENS[model]

        owner_info = self.query_assistant_owner(assistant_uuid, session)
        if not owner_info:
            logger.warning('assistant %s has no owner', assistant_uuid)
            return
        if owner_info['org_id'] != -1:
            self.org_stats[owner_info['org_id']][model]['input_tokens'] += usage.get('input_tokens') or 0
            self.org_stats[owner_info['org_id']][model]['output_tokens'] += usage.get('output_tokens') or 0
        else:
            self.owner_stats[owner_info['owner_id']][model]['input_tokens'] += usage.get('input_tokens') or 0
            self.owner_stats[owner_info['owner_id']][model]['output_tokens'] += usage.get('output_tokens') or 0

    def receive(self):
        logger.info('Starts to receive ai calls...')
        subscriber = self._redis_client.get_subscriber(self.channel)

        while True:
            try:
                message = subscriber.get_message()
                if message is not None:
                    try:
                        usage_info = json.loads(message['data'])
                    except:
                        logger.warning('log_ai_model_usage message invalid')
                        continue
                    session = self._db_session_class()
                    logger.debug('usage_info %s', usage_info)
                    try:
                        with self.stats_lock:
                            self.save_to_memory(usage_info, session)
                    except Exception as e:
                        logger.exception('save usage_info %s to memory error %s', usage_info, e)
                    finally:
                        session.close()
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber(self.channel)

    def get_assistant_cache_key(self, assistant_uuid):
        return f'assistant:{assistant_uuid}:owner'

    def query_assistant_owner(self, assistant_uuid, session):
        cache_key = self.get_assistant_cache_key(assistant_uuid)
        owner_info = redis_cache.get(cache_key)
        if owner_info:
            return json.loads(owner_info)
        sql = '''
            SELECT aao.assistant_uuid, aao.owner, w.org_id FROM ai_assistant_owner aao
            JOIN workspaces w ON aao.owner=w.owner
            WHERE aao.assistant_uuid=:assistant_uuid
        '''
        results = session.execute(text(sql), {'assistant_uuid': assistant_uuid})
        row = results.fetchone()
        if not row:
            return None
        owner_info = {'org_id': row.org_id, 'owner_id': row.owner}
        redis_cache.set(cache_key, json.dumps(owner_info), timeout=self.owner_info_cache_timeout)
        return owner_info

    def stats_worker(self):
        if not self.org_stats and not self.owner_stats:
            logger.info('There are no stats')
            return
        with self.stats_lock:
            org_stats = deepcopy(self.org_stats)
            owner_stats = deepcopy(self.owner_stats)

        logger.info('There are %s org stats', len(org_stats))
        logger.info('There are %s owner stats (including groups with -1 org_id)', len(owner_stats))

        month = datetime.today().replace(day=1).date()

        team_data = []
        team_sql = '''
        INSERT INTO `stats_ai_by_team`(`org_id`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`) 
        VALUES (:org_id, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                                `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                                `cost`=`cost`+VALUES(`cost`),
                                `updated_at`=VALUES(`updated_at`)
        '''
        for org_id, models_dict in org_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('org %s model %s, input_tokens %s cost %s, output_tokens %s cost %s', org_id, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'org_id': org_id,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                team_data.append(params)

        owner_data = []
        owner_sql = '''
        INSERT INTO `stats_ai_by_owner`(`owner_id`, `month`, `model`, `input_tokens`, `output_tokens`, `cost`, `created_at`, `updated_at`) 
        VALUES (:owner_id, :month, :model, :input_tokens, :output_tokens, :cost, :created_at, :updated_at)
        ON DUPLICATE KEY UPDATE `input_tokens`=`input_tokens`+VALUES(`input_tokens`),
                                `output_tokens`=`output_tokens`+VALUES(`output_tokens`),
                                `cost`=`cost`+VALUES(`cost`),
                                `updated_at`=VALUES(`updated_at`)
        '''
        for owner_id, models_dict in owner_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('owner %s model %s, input_tokens %s cost %s, output_tokens %s cost %s', owner_id, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'owner_id': owner_id,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                owner_data.append(params)

        session = self._db_session_class()
        try:
            if team_data:
                session.execute(text(team_sql), team_data)
            if owner_data:
                session.execute(text(owner_sql), owner_data)
            session.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            session.close()

    def stats(self):
        sched = BlockingScheduler()
        # fire per 5 mins
        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='*/5', misfire_grace_time=600)
        def timed_job():
            logger.info('Starts to stats ai calls in memory...')
            self.stats_worker()

        sched.start()

    def clean(self):
        sched = BlockingScheduler()
        # fire at 0 o'clock in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='0', misfire_grace_time=600)
        def timed_job():
            logger.info('Starts to clean old stats ai...')
            session = self._db_session_class()
            sql1 = "DELETE FORM `stats_ai_by_team` WHERE `month` < :clean_month"
            sql2 = "DELETE FORM `stats_ai_by_owner` WHERE `month` < :clean_month"
            clean_month = (datetime.now() - relativedelta.relativedelta(months=self.keep_months)).strftime('%Y-%m-01')
            try:
                session.execute(text(sql1), {'clean_month': clean_month})
                session.execute(text(sql2), {'clean_month': clean_month})
            except Exception as e:
                logger.exception(e)
            finally:
                session.close()

        sched.start()

    def start(self):
        if not self._enabled:
            logger.warning('Can not stats AI: it is not enabled!')
            return
        Thread(target=self.receive, daemon=True).start()
        Thread(target=self.stats, daemon=True).start()
        Thread(target=self.clean, daemon=True).start()
