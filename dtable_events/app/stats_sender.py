import json
import logging

from dtable_events.app.event_redis import RedisClient

logger = logging.getLogger(__name__)


class StatsSender:

    CDS_CHANNEL = 'stats_cds'

    def __init__(self):
        self.config = None
        self.redis_client = None

    def init_config(self, config):
        self._redis_client = RedisClient(config)

    def send(self, channel: str, info: dict):
        try:
            self._redis_client.connection.publish(channel, json.dumps(info))
        except Exception as e:
            logger.warning('send info to channel: %s error: %s', channel, e)

    def get_stats_cds_info_template(self):
        return {
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


stats_sender = StatsSender()
