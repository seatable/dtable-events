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
        airtable_api_key = context['airtable_api_key']
        airtable_base_id = context['airtable_base_id']
        table_names = context.get('table_names', [])
        first_columns = context.get('first_columns', [])
        links = context.get('links', [])
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

        convertor = AirtableConvertor(
            airtable_api_key=airtable_api_key,
            airtable_base_id=airtable_base_id,
            base=dtable_server_api,
            table_names=table_names,
            first_columns=first_columns,
            links=links,
        )
        
        convertor.convert_airtable_to_seatable()

    except Exception as e:
        error_msg = f'Failed to import Airtable: {str(e)}'
        dtable_io_logger.error(error_msg)
        raise Exception(error_msg)
