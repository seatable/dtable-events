import logging

from sqlalchemy import text


logger = logging.getLogger(__name__)

class StatsManager:

    def __init__(self, db_session) -> None:
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
        self.db_session = db_session

    def __getitem__(self, key):
        return self.info.get(key)

    def __setitem__(self, key, value):
        self.info[key] = value

    def __delitem__(self, key):
        del self.info[key]

    def save(self):
        try:
            sql = '''
            INSERT INTO
              common_dataset_stats(
                  org_id,
                  dataset_id,
                  import_or_sync,
                  operator,
                  started_at,
                  finished_at,
                  src_dtable_uuid,
                  src_table_id,
                  src_view_id,
                  dst_dtable_uuid,
                  dst_table_id,
                  to_be_appended_rows_count,
                  to_be_updated_rows_count,
                  to_be_deleted_rows_count,
                  appended_rows_count,
                  updated_rows_count,
                  deleted_rows_count,
                  columns_count,
                  link_formula_columns_count,
                  is_success,
                  error
              )
              VALUES
              (
                  :org_id,
                  :dataset_id,
                  :import_or_sync,
                  :operator,
                  :started_at,
                  :finished_at,
                  :src_dtable_uuid,
                  :src_table_id,
                  :src_view_id,
                  :dst_dtable_uuid,
                  :dst_table_id,
                  :to_be_appended_rows_count,
                  :to_be_updated_rows_count,
                  :to_be_deleted_rows_count,
                  :appended_rows_count,
                  :updated_rows_count,
                  :deleted_rows_count,
                  :columns_count,
                  :link_formula_columns_count,
                  :is_success,
                  :error
              )
            '''
            self.db_session.execute(text(sql), self.info)
            self.db_session.commit()
        except Exception as e:
            logger.exception('save common dataset sync stats: %s error: %s', self.info, e)
