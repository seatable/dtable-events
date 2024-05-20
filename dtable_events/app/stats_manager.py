import json
import logging
from datetime import datetime

import redis

from dtable_events.app.event_redis import RedisClient


logger = logging.getLogger(__name__)

class CommonDatasetStatsWorker:
    key = 'stats_cds'

    def __init__(self, redis_client: RedisClient) -> None:
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

    def __getitem__(self, key):
        return self.info.get(key)

    def __setitem__(self, key, value):
        self.info[key] = value

    def __delitem__(self, key):
        del self.info[key]

    def save(self):
        try:
            if self.info.get('started_at') and isinstance(self.info.get('started_at'), datetime):
                self.info['started_at'] = self.info['started_at'].isoformat()
            if self.info.get('finished_at') and isinstance(self.info.get('finished_at'), datetime):
                self.info['finished_at'] = self.info['finished_at'].isoformat()
            self.redis_client.publish(self.key, json.dumps(self.info))
        except Exception as e:
            logger.warning('publish stats to %s error: %s', self.key, e)


class RedisStasWorkerFactory:

    def __init__(self):
        self.redis_client = None

    def init_redis(self, config):
        self.redis_client = RedisClient(config)

    def get_common_dataset_stats_worker(self):
        return CommonDatasetStatsWorker(self.redis_client)


redis_stats_worker_factory = RedisStasWorkerFactory()
