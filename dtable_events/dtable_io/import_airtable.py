# -*- coding: utf-8 -*-
import time
from dtable_events.dtable_io import dtable_io_logger
from dtable_events.dtable_io.airtable_convertor import AirtableConvertor
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, INNER_DTABLE_SERVER_URL


def import_airtable(context):
    try:
        username = context['username']
        dtable_uuid = context['dtable_uuid'] 
        airtable_access_token = context['airtable_access_token']
        airtable_base_id = context['airtable_base_id']
        workspace_id = context.get('workspace_id')
        repo_id = context.get('repo_id')
        
        dtable_server_api = DTableServerAPI(
            username, 
            dtable_uuid, 
            INNER_DTABLE_SERVER_URL, 
            server_url=DTABLE_WEB_SERVICE_URL,
            workspace_id=workspace_id,
            repo_id=repo_id
        )

        default_table_id = '0000'
        temp_table_name = f'Table1__tmp__{int(time.time())}'
        dtable_server_api.rename_table(temp_table_name, default_table_id)

        convertor = AirtableConvertor(
            airtable_access_token=airtable_access_token,
            airtable_base_id=airtable_base_id,
            base=dtable_server_api,
        )
        
        convertor.convert_airtable_to_seatable()

        try:
            metadata = dtable_server_api.get_metadata() or {}
            tables = metadata.get('tables') or []
            has_temp_table = any(table.get('_id') == default_table_id for table in tables)
            if has_temp_table and len(tables) > 1:
                dtable_server_api.delete_table(table_id=default_table_id)
                dtable_io_logger.info('Deleted temporary table "%s" after Airtable import', temp_table_name)
        except Exception as e:
            dtable_io_logger.warning('Failed to delete temporary table "%s": %s', temp_table_name, e)

    except Exception as e:
        error_msg = f'Import Airtable error: {str(e)}'
        dtable_io_logger.error(error_msg)
        raise Exception(error_msg)
