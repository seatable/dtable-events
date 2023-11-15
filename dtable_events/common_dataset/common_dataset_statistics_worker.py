import logging

from dtable_events.system_bases.system_bases import system_bases_manager

logger = logging.getLogger(__name__)


class CommonDatasetStatisticWorker:

    def __init__(self):
        self.base_name = 'CDS-statistics'
        self.table_name = 'CDS-statistics'
        self.stats_data = {}

    def set_stats_data(self, attr, value):
        self.stats_data[attr] = value

    def _record_stats_data(self):
        if not system_bases_manager.is_upgrade_done:
            return

        org_id = self.stats_data.get('org_id', -1)
        sync_id = self.stats_data.get('sync_id')
        import_or_sync = self.stats_data.get('import_or_sync')
        sync_type = self.stats_data.get('sync_type')
        started_at = str(self.stats_data.get('started_at'))
        finished_at = str(self.stats_data.get('finished_at'))
        is_success = self.stats_data.get('is_success')
        to_be_appended_rows_count = self.stats_data.get('to_be_appended_rows_count', 0)
        to_be_updated_rows_count = self.stats_data.get('to_be_updated_rows_count', 0)
        to_be_deleted_rows_count = self.stats_data.get('to_be_deleted_rows_count', 0)
        appended_rows_count = self.stats_data.get('appended_rows_count', 0)
        updated_rows_count = self.stats_data.get('updated_rows_count', 0)
        deleted_rows_count = self.stats_data.get('deleted_rows_count', 0)
        columns_count = self.stats_data.get('columns_count', 0)
        link_formula_columns_count = self.stats_data.get('link_formula_columns_count', 0)
        error = self.stats_data.get('error')

        row_data = {
            'org_id': org_id,
            'sync_id': sync_id,
            'import_or_sync': import_or_sync,
            'sync_type': sync_type,
            'started_at': started_at,
            'finished_at': finished_at,
            'is_success': is_success,
            'to_be_appended_rows_count': to_be_appended_rows_count,
            'to_be_updated_rows_count': to_be_updated_rows_count,
            'to_be_deleted_rows_count': to_be_deleted_rows_count,
            'appended_rows_count': appended_rows_count,
            'updated_rows_count': updated_rows_count,
            'deleted_rows_count': deleted_rows_count,
            'columns_count': columns_count,
            'link_formula_columns_count': link_formula_columns_count,
            'error': error
        }

        dtable_db_api = system_bases_manager.get_dtable_db_api_by_name(self.base_name)
        if  not dtable_db_api:
            return
        try:
            dtable_db_api.insert_rows(self.table_name, [row_data])
        except Exception as e:
            logger.error('append CDS stats base: %s table: %s error: %s', self.base_name, self.table_name, e)

    def record_stats_data(self):
        try:
            self._record_stats_data()
        except Exception as e:
            logger.exception('record sync_id: %s error: %s', self.stats_data.get('sync_id'), e)
