# -*- coding: utf-8 -*-
import time

from dtable_events.dtable_io import dtable_io_logger
from dtable_events.dtable_io.airtable_convertor import AirtableConvertor
from dtable_events.utils.dtable_server_api import DTableServerAPI, BaseExceedsException
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_SERVER_URL


def check_quota(org_id, username, import_rows_count, import_asset_size):
    """
    Check if importing would exceed the rows limit or asset quota.

    Raises:
        BaseExceedsException: If import would exceed limit
    """
    dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
    try:
        if org_id and org_id != -1:
            result = dtable_web_api.internal_storage_quota(org_id=org_id)
        else:
            result = dtable_web_api.internal_storage_quota(username=username)
    except Exception as e:
        dtable_io_logger.warning('Failed to get quota info: %s', e)
        return

    # Check rows limit
    row_limit = result.get('row_limit', -1)
    row_usage = result.get('row_usage', 0)
    if row_limit > 0 and (row_usage + import_rows_count) > row_limit:
        dtable_io_logger.warning(
            'Airtable import would exceed rows limit. current: %d, importing: %d, limit: %d',
            row_usage, import_rows_count, row_limit
        )
        raise BaseExceedsException('exceed_rows_limit', 'Exceed the rows limit')

    # Check asset quota
    asset_quota = result.get('asset_quota', -1)
    asset_usage = result.get('asset_usage', 0)
    if asset_quota > 0 and (asset_usage + import_asset_size) > asset_quota:
        dtable_io_logger.warning(
            'Airtable import would exceed asset quota. current: %d, importing: %d, quota: %d',
            asset_usage, import_asset_size, asset_quota
        )
        raise BaseExceedsException('exceed_asset_quota', 'Exceed the asset quota')


def import_airtable(context):
    try:
        username = context['username']
        dtable_uuid = context['dtable_uuid']
        airtable_access_token = context['airtable_access_token']
        airtable_base_id = context['airtable_base_id']
        workspace_id = context.get('workspace_id')
        repo_id = context.get('repo_id')
        org_id = context.get('org_id')

        dtable_server_api = DTableServerAPI(
            username,
            dtable_uuid,
            INNER_DTABLE_SERVER_URL,
            server_url=DTABLE_WEB_SERVICE_URL,
            workspace_id=workspace_id,
            repo_id=repo_id
        )

        convertor = AirtableConvertor(
            airtable_access_token=airtable_access_token,
            airtable_base_id=airtable_base_id,
            base=dtable_server_api,
        )
        convertor.prepare_import()

        # Check rows limit and asset quota before importing
        if convertor.airtable_row_map:
            total_rows = sum(len(rows) for rows in convertor.airtable_row_map.values())
            total_asset_size = convertor.get_total_asset_size()
            if total_rows > 0 or total_asset_size > 0:
                check_quota(org_id, username, total_rows, total_asset_size)

        default_table_id = '0000'
        temp_table_name = f'Table1__tmp__{int(time.time())}'
        dtable_server_api.rename_table(temp_table_name, default_table_id)

        convertor.execute_import()

        try:
            metadata = dtable_server_api.get_metadata() or {}
            tables = metadata.get('tables') or []
            has_temp_table = any(table.get('_id') == default_table_id for table in tables)
            if has_temp_table and len(tables) > 1:
                dtable_server_api.delete_table(table_id=default_table_id)
                dtable_io_logger.info('Deleted temporary table "%s" after Airtable import', temp_table_name)
        except Exception as e:
            dtable_io_logger.warning('Failed to delete temporary table "%s": %s', temp_table_name, e)

    except BaseExceedsException as e:
        error_msg = f'Import Airtable error: {e.error_msg}'
        dtable_io_logger.error('Airtable import failed: %s', e.error_msg)
        raise Exception(error_msg)
    except ConnectionError as e:
        status_code = e.args[0]
        response_text = e.args[1] if len(e.args) > 1 else str(e)
        if status_code == 401:
            error_msg = 'Import Airtable error: Invalid token'
        elif status_code == 403:
            error_msg = 'Import Airtable error: Insufficient permission'
        elif status_code == 404:
            error_msg = 'Import Airtable error: Airtable base not found'
        else:
            error_msg = f'Import Airtable error: Airtable API error ({status_code})'
        dtable_io_logger.error('Airtable API error: %s - %s', status_code, response_text)
        raise Exception(error_msg)
    except Exception as e:
        error_msg = f'Import Airtable error: {str(e)}'
        dtable_io_logger.error(error_msg)
        raise Exception(error_msg)
