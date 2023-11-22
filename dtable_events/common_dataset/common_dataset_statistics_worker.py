import json
import logging

from dtable_events.app.config import ENABLE_SYSTEM_BASES
from dtable_events.app.event_redis import redis_cache
from dtable_events.system_bases.constants import CDS_STATISTICS_MSG_KEY, CDS_STATISTICS_BASE_NAME
from dtable_events.system_bases.system_bases import system_bases_manager

logger = logging.getLogger(__name__)


class CommonDatasetStatisticWorker:

    def __init__(self):
        self.stats_data = {}

    def set_stats_data(self, attr, value):
        self.stats_data[attr] = value

    def _record_stats_data(self):
        # must enable system bases
        if not ENABLE_SYSTEM_BASES:
            return
        # must CDS-stats base be ready
        base = system_bases_manager.get_base_by_name(CDS_STATISTICS_BASE_NAME)
        if not base.is_ready:
            return
        row_data = {
            'org_id': self.stats_data.get('org_id', -1),
            'sync_id': self.stats_data.get('sync_id'),
            'import_or_sync': self.stats_data.get('import_or_sync'),
            'sync_type': self.stats_data.get('sync_type'),
            'started_at': str(self.stats_data.get('started_at')),
            'finished_at': str(self.stats_data.get('finished_at')),
            'is_success': self.stats_data.get('is_success'),
            'to_be_appended_rows_count': self.stats_data.get('to_be_appended_rows_count', 0),
            'to_be_updated_rows_count': self.stats_data.get('to_be_updated_rows_count', 0),
            'to_be_deleted_rows_count': self.stats_data.get('appended_rows_count', 0),
            'appended_rows_count': self.stats_data.get('appended_rows_count', 0),
            'updated_rows_count': self.stats_data.get('updated_rows_count', 0),
            'deleted_rows_count': self.stats_data.get('deleted_rows_count', 0),
            'columns_count': self.stats_data.get('columns_count', 0),
            'link_formula_columns_count': self.stats_data.get('link_formula_columns_count', 0),
            'error': self.stats_data.get('error')
        }

        try:
            redis_cache._redis_client.connection.rpush(CDS_STATISTICS_MSG_KEY, json.dumps(row_data))
            redis_cache._redis_client.connection.expire(CDS_STATISTICS_MSG_KEY, 60 * 60)
        except Exception as e:
            logger.error('append CDS stats base: %s table: %s error: %s', self.base_name, self.table_name, e)

    def record_stats_data(self):
        try:
            self._record_stats_data()
        except Exception as e:
            logger.exception('record sync_id: %s error: %s', self.stats_data.get('sync_id'), e)
