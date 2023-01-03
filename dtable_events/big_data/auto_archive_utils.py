import logging
import json
from datetime import datetime
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_36_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.sql_generator import BaseSQLGenerator

logger = logging.getLogger(__name__)
def update_last_run_time(task_id, db_session):

    cmd = "UPDATE dtable_auto_archive_task SET last_run_time=:new_time WHERE id=:task_id"
    db_session.execute(cmd, {'new_time': datetime.utcnow(), 'task_id': task_id})

def set_invalid(task_id, db_session):
    sql = "UPDATE dtable_auto_archive_task SET is_valid=:is_valid WHERE id=:task_id"
    try:
        db_session.execute(sql, {'is_valid': 0, 'task_id': task_id})
    except Exception as e:
        logger.error(e)

def meet_condition(run_condition, details):
    if run_condition == 'per_day':
        run_hour = details.get('run_hour', None)
        cur_datetime = datetime.now()
        cur_hour = int(cur_datetime.hour)
        try:
            if int(run_hour) == cur_hour:
                return True
        except:
            return False


    return False


def run_dtable_auto_archive_task(task, db_session):

    task_id = task[0]
    run_condition = task[1]
    table_id = task[2]
    view_id = task[3]
    last_run_time = task[4]
    dtable_uuid = task[5]
    details = task[6]
    creator = task[7]
    try:
        details = json.loads(details)
        if not meet_condition(run_condition, details):
            return
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        dtable_server_url = get_inner_dtable_server_url()
        seatable = DTableServerAPI(creator, dtable_uuid, dtable_server_url)
        current_table, current_view = None, None
        metadata = seatable.get_metadata()
        for table in metadata['tables']:
            if table.get('_id') == table_id:
                current_table = table
                break

        if not current_table:
            set_invalid(task_id, db_session)
            return

        for view in current_table['views']:
            if view.get('_id') == view_id:
                current_view = view
                break

        if not current_view:
            set_invalid(task_id, db_session)
            return

        table_name = current_table.get('name')
        filter_conditions = {
            "filters": current_view.get('filters') or [],
            "filter_conjunction": current_view.get('filter_conjunction') or 'And',
        }

        columns = seatable.list_columns(table_name)
        sql_generator = BaseSQLGenerator(table_name, columns, filter_conditions=filter_conditions)
        where_clause = sql_generator.get_where_clause()
        seatable.archive_view(table_name, where_clause)

    except Exception as e:
        logger.error(e)
        set_invalid(task_id, db_session)
        return

    update_last_run_time(task_id, db_session)
