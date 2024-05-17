import json
import logging
from datetime import datetime

import redis

from dtable_events.utils import parse_bool


logger = logging.getLogger(__name__)

class CommonDatasetStatsWorker:

    def __init__(self, redis_client: redis.Redis, stats_key) -> None:
        self.info = {
            'org_id': None,
            'dataset_id': None,
            'src_dtable_uuid': None,
            'src_table_id': None,
            'src_view_id': None,
            'dst_dtable_uuid': None,
            'dst_table_id': None,
            'import_or_sync': None,
            'operator': None,
            'started_at': None,
            'finished_at': None,
            'to_be_appended_rows_count': 0,
            'to_be_updated_rows_count': 0,
            'to_be_deleted_rows_count': 0,
            'appended_rows_count': 0,
            'updated_rows_count': 0,
            'deleted_rows_count': 0,
            'columns_count': 0,
            'link_formula_columns_count': 0,
            'is_success': None,
            'error': None
        }
        self.redis_client = redis_client
        self.key = stats_key

    def __getitem__(self, key):
        return self.info.get(key)

    def __setitem__(self, key, value):
        self.info[key] = value

    def __delitem__(self, key):
        del self.info[key]

    def save(self):
        if not self.redis_client:
            return
        if self.info.get('started_at') and isinstance(self.info.get('started_at'), datetime):
            self.info['started_at'] = self.info['started_at'].isoformat()
        if self.info.get('finished_at') and isinstance(self.info.get('finished_at'), datetime):
            self.info['finished_at'] = self.info['finished_at'].isoformat()
        try:
            self.redis_client.rpush(self.key, json.dumps(self.info))
        except redis.RedisError as e:
            logger.warning('common dataset stats redis error: %s', e)
        except Exception as e:
            logger.exception('save common dataset stats: %s error: %s', self.info, e)


class RedisStasWorkerFactory:

    def __init__(self):
        self.redis_client = None
        self.enabled = False

    def init_redis(self, config, socket_connect_timeout=30, socket_timeout=None):
        self._host = '127.0.0.1'
        self._port = 6379
        self._password = None
        self._db = 0
        self._cds_stats_key = 'CDS-STATAS'
        self._parse_config(config)

        if self.enabled:
            self.redis_client = redis.Redis(
                host=self._host, port=self._port, password=self._password, db=self._db,
                socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout,
                decode_responses=True
            )

    def _parse_config(self, config):
        if config.has_option('STATS-REDIS', 'enabled'):
            self.enabled = parse_bool(config.get('STATS-REDIS', 'enabled', fallback='false'))

        if config.has_option('STATS-REDIS', 'host'):
            self._host = config.get('STATS-REDIS', 'host')

        if config.has_option('STATS-REDIS', 'port'):
            self._port = config.getint('STATS-REDIS', 'port')

        if config.has_option('STATS-REDIS', 'password'):
            self._password = config.get('STATS-REDIS', 'password')

        if config.has_option('STATS-REDIS', 'db'):
            try:
                self._db = int(config.get('STATS-REDIS', 'db'))
            except:
                logger.warning('stats redis db: %s invalid', config.get('STATS-REDIS', 'db'))
                self._db = 0

        if config.has_option('STATS-REDIS', 'cds_stats_key'):
            self._cds_stats_key = config.get('STATS-REDIS', 'cds_stats_key', fallback='CDS-STATAS')

    def get_common_dataset_stats_worker(self):
        return CommonDatasetStatsWorker(self.redis_client, self._cds_stats_key)


redis_stats_worker_factory = RedisStasWorkerFactory()
