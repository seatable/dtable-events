import logging
import os
import re
import time
from datetime import datetime
from uuid import uuid4

import requests

from seaserv import seafile_api

from dtable_events.app.config import NEW_DTABLE_IN_STORAGE_SERVER
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_32_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.storage_backend import storage_backend

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()

class SystemBasesManager:

    def __init__(self, config):
        self.owner = 'system base'
        self.is_upgrade_done = False
        self.session_class = init_db_session_class(config)
        self.versions = [
            '0.0.1',
            '0.0.2'
        ]
        self.version_base_name = 'version'
        self.owner = 'system base'

        self.version_dtable_uuid = None

    def get_base_by_type(self, base_type) -> DTableServerAPI:
        if base_type == 'version':
            with self.session_class() as session:
                sql = '''
                SELECT uuid FROM dtables d
                JOIN workspaces w ON d.workspace_id=w.id
                WHERE w.owner=:owner AND d.name=:base_name
                '''
                results = session.execute(sql)
                for item in results:
                    self.version_dtable_uuid = item.uuid
                    break
                if not self.version_dtable_uuid:
                    return None
                return DTableServerAPI('dtable-events', self.version_dtable_uuid, dtable_server_url)

    def ping(self) -> bool:
        url = f"{dtable_server_url.strip('/')}/ping/"
        try:
            resp = requests.get(url, timeout=5)
        except:
            return False
        if resp.status_code != 200:
            return False
        return resp.text.strip('') == 'pong'

    def upgrade(self):
        while True:
            pong = self.ping()
            if pong:
                break
            logger.info('dtable-server no pong')
            time.sleep(5)
        logger.info('dtable-server connected')
        version_base = self.get_base_by_type('version')
        if not version_base:
            raise Exception('Base not found')
        files = os.listdir(os.path.join(os.path.dirname(__file__), 'upgrade'))
        version_re = r'\d+.\d+.\d+'
        for file in files:
            if re.match(version_re, file):
                pass
