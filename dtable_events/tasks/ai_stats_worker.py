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
from dtable_events.app.event_redis import RedisClient
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, uuid_str_to_36_chars

logger = logging.getLogger(__name__)


class AIStatsWorker:

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self._redis_client = RedisClient(config)
        self.stats_lock = Lock()
        self.assistant_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        self.channel = 'log_ai_model_usage'
        self.keep_months = 3
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

    def save_to_memory(self, usage_info):
        if not usage_info.get('model') or not usage_info.get('assistant_uuid'):
            return

        assistant_uuid = uuid_str_to_36_chars(usage_info.get('assistant_uuid'))
        model = usage_info['model']
        usage = usage_info.get('usage')

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

        self.assistant_stats[assistant_uuid][model]['input_tokens'] += usage.get('input_tokens') or 0
        self.assistant_stats[assistant_uuid][model]['output_tokens'] += usage.get('output_tokens') or 0

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
                    logger.debug('usage_info %s', usage_info)
                    with self.stats_lock:
                        self.save_to_memory(usage_info)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logger.error('Failed get message from redis: %s' % e)
                subscriber = self._redis_client.get_subscriber(self.channel)

    def query_assistant_owners(self, assistant_uuids, session):
        sql = '''
            SELECT aao.assistant_uuid, aao.owner, w.org_id FROM ai_assistant_owner aao
            JOIN workspaces w ON aao.owner=w.owner
            WHERE aao.assistant_uuid IN :assistant_uuids
        '''
        results = session.execute(text(sql), {'assistant_uuids': assistant_uuids})
        ret = {item.assistant_uuid: {'org_id': item.org_id, 'owner': item.owner} for item in results}
        return ret

    def stats_worker(self):
        if not self.assistant_stats:
            logger.info('There are no assistant stats')
            return
        with self.stats_lock:
            assistant_stats = deepcopy(self.assistant_stats)
            self.assistant_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))

        logger.info('There are %s assistant stats', len(assistant_stats))

        session = self._db_session_class()
        assistant_owners_dict = self.query_assistant_owners(list(assistant_stats.keys()), session)

        org_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))
        user_stats = defaultdict(lambda: defaultdict(lambda: {'input_tokens': 0, 'output_tokens': 0}))

        for assistant_uuid, models_dict in assistant_stats.items():
            owner_info = assistant_owners_dict.get(assistant_uuid)
            if not owner_info:
                logger.warning('assistant %s not found in db', assistant_uuid)
                continue
            org_id = owner_info.get('org_id')
            owner = owner_info.get('owner')
            owner_stat = org_stats[org_id] if org_id != -1 else user_stats[owner]
            for model, usage in models_dict.items():
                owner_stat[model]['input_tokens'] += usage.get('input_tokens') or 0
                owner_stat[model]['output_tokens'] += usage.get('output_tokens') or 0

        logger.info('There are %s org stats', len(org_stats))
        logger.info('There are %s user stats (including groups with -1 org_id)', len(user_stats))

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

                if model not in AI_PRICES:
                    logger.info('org %s price of model %s not defined', org_id, model)
                    continue
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
        for username, models_dict in user_stats.items():
            for model, usage in models_dict.items():
                input_tokens = usage.get('input_tokens') or 0
                output_tokens = usage.get('output_tokens') or 0

                if model not in AI_PRICES:
                    logger.info('user %s price of model %s not defined', username, model)
                    continue
                input_tokens_price = AI_PRICES[model].get('input_tokens_1k') or 0
                output_tokens_price = AI_PRICES[model].get('output_tokens_1k') or 0
                input_cost = input_tokens_price * (input_tokens / 1000)
                output_cost = output_tokens_price * (output_tokens / 1000)
                logger.info('user %s model %s, input_tokens %s cost %s, output_tokens %s cost %s', username, model, input_tokens, input_cost, output_tokens, output_cost)

                params = {
                    'owner_id': username,
                    'month': month,
                    'model': model,
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens,
                    'cost': input_cost + output_cost,
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                owner_data.append(params)

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
        # fire at 0,30 in every hour
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
