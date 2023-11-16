import logging
import os
import time
import subprocess
import threading
from datetime import datetime

from seaserv import seafile_api

from dtable_events.app.config import INNER_DTABLE_DB_URL, SYSTEM_BASES_OWNER, ENABLE_SYSTEM_BASES
from dtable_events.db import init_db_session_class
from dtable_events.system_bases.constants import VERSION_BASE_NAME, VERSION_TABLE_NAME, CDS_STATISTICS_BASE_NAME
from dtable_events.system_bases.bases import VerionBaseManager, CDSStatisticsBaseManager
from dtable_events.utils import get_inner_dtable_server_url, get_python_executable, uuid_str_to_36_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()

class SystemBasesManager:

    def __init__(self):
        self.versions = [
            
        ]

        self.bases_manager_map = {
            VERSION_BASE_NAME: VerionBaseManager,
            CDS_STATISTICS_BASE_NAME: CDSStatisticsBaseManager
        }

        self.current_version = ''

        self.base_uuids_dict = {}

    def init_config(self, config):
        self.session_class = init_db_session_class(config)

    def create_workspace(self):
        repo_id = seafile_api.create_repo(
            "My Workspace",
            "My Workspace",
            "dtable@seafile"
        )
        now = datetime.now()

        with self.session_class() as session:
            # workspace
            sql = '''
            INSERT INTO workspaces(owner, repo_id, created_at, org_id) VALUES
            (:owner, :repo_id, :created_at, -1)
            '''
            result = session.execute(sql, {
                'owner': SYSTEM_BASES_OWNER,
                'repo_id': repo_id,
                'created_at': now
            })
            workspace_id = result.lastrowid
        return workspace_id

    def get_workspace_id(self):
        with self.session_class() as session:
            sql = "SELECT id FROM workspaces WHERE owner=:owner"
            result = session.execute(sql, {'owner': SYSTEM_BASES_OWNER}).fetch_one()
            if not result:
                return None
            return result.id

    def request_current_version(self):
        workspace_id = self.get_workspace_id()
        if not workspace_id:
            self.create_workspace()
            return '0.0.0'
        sql = f"SELECT version FROM `{VERSION_TABLE_NAME}` LIMIT 1"
        version_dtable_db_api = DTableDBAPI('dtable-events', 'version_dtable_server_api.dtable_uuid', INNER_DTABLE_DB_URL)
        try:
            results, _ = version_dtable_db_api.query(sql, convert=True, server_only=True)
        except Exception as e:
            logger.error('query version error: %s', e)
            return None
        if not results:
            return None
        return results[0]['version']

    def get_base_manager(self, name):
        return self.bases_manager_map.get(name)

    @staticmethod
    def comp(v1, v2) -> bool:
        """
        :return: v1 > v2
        """
        v1 = tuple(int(v) for v in v1.split('.'))
        v2 = tuple(int(v) for v in v2.split('.'))
        return v1 > v2

    def _upgrade(self):
        sleep = 5
        while True:
            try:
                dtable_server_pong = DTableServerAPI.ping(dtable_server_url, timeout=5)
                dtable_db_pong = DTableDBAPI.ping(INNER_DTABLE_DB_URL, timeout=5)
            except:
                logger.info('dtable-server or dtable-db not ready will try in %ss', sleep)
                time.sleep(sleep)
            else:
                if not dtable_server_pong or not dtable_db_pong:
                    logger.info('dtable-server or dtable-db not ready will try in %ss', sleep)
                    time.sleep(sleep)
                    continue
                break
        logger.info('dtable-server and dtable-db ready')

        self.current_version = self.request_current_version()
        if self.current_version is None:
            logger.error('workspace created but no version found')
            return
        logger.info('current_version: %s', self.current_version)
        for version in self.versions:
            pass

    def upgrade(self):
        try:
            self._upgrade()
        except Exception as e:
            logger.exception('upgrade error: %s', e)


system_bases_manager = SystemBasesManager()
