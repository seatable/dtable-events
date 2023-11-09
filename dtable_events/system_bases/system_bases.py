import logging
import os
import time
import subprocess

import requests

from dtable_events.app.config import central_conf_dir, INNER_DTABLE_DB_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_inner_dtable_server_url, uuid_str_to_32_chars, get_python_executable
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI
from dtable_events.utils.storage_backend import storage_backend

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()

class SystemBasesManager:

    def __init__(self):
        self.is_upgrade_done = False
        self.versions = [
            '4.3.0'
        ]

        self.current_version = ''

        self.version_base_name = 'version'
        self.version_table_name = 'version'

        self.owner = 'system bases'

        self.version_dtable_uuid = None

    def init_config(self, config):
        self.session_class = init_db_session_class(config)

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

    def _upgrade_version(self, version):
        file = f'{version}.py'
        file_path = os.path.join(os.path.dirname(__file__), 'upgrade', file)
        if not os.path.isfile(file_path):
            logger.error('file %s not found', file_path)
            return Exception(f'version {version} file not found')
        python_exec = get_python_executable()
        conf_path = os.path.join(central_conf_dir, 'dtable-events.conf')
        cmd = [python_exec, file_path, '--config-file', conf_path]
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        return result

    def upgrade_version(self, version):
        logger.info('start to upgrade system bases version: %s', version)
        try:
            result = self._upgrade_version(version)
        except Exception as e:
            logger.exception('upgrade version: %s error: %s', version, e)
        if result.returncode != 0:
            logger.error('upgrade version: %s error code: %s content: %s', version, result.returncode, result.stdout.decode())
        else:
            self.is_upgrade_done = True
            logger.info('upgrade system bases version: %s success', version)

    def request_current_version(self):
        sql = f"SELECT version FROM `{self.version_table_name}` LIMIT 1"
        version_dtable_db_api = DTableDBAPI('dtable-events', self.version_dtable_uuid)
        try:
            results, _ = version_dtable_db_api.query(sql, convert=True, server_only=True)
        except Exception as e:
            logger.error('query version error: %s', e)
            return None
        if not results:
            return None
        return results[0]['version']

    @staticmethod
    def comp(v1, v2) -> bool:
        """
        :return: v1 > v2
        """
        v1s = v1.split('.')
        v2s = v2.split('.')
        for i in range(len(v1s)):
            if int(v1s[i]) > int(v2s[i]):
                return True
            elif int(v1s[i]) < int(v2s[i]):
                return False
        return False

    def upgrade(self):
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
        if len(self.versions) == 1:
            self.upgrade_version(self.versions[0])
        else:
            self.current_version = self.request_current_version()
            logger.info('current_version: ', self.current_version)
            for version in self.versions:
               if not self.comp(version, self.current_version):
                   logger.info('version %s has been upgraded!', version)
               self.upgrade_version(version)
        logger.info('all upgrade done!')
        self.is_upgrade_done = True


system_bases_manager = SystemBasesManager()
