import logging
import os
import time
import subprocess
import threading

from dtable_events.app.config import central_conf_dir, INNER_DTABLE_DB_URL, SYSTEM_BASES_OWNER, ENABLE_SYSTEM_BASES
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_inner_dtable_server_url, get_python_executable, uuid_str_to_36_chars
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

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

        self.owner = SYSTEM_BASES_OWNER

        self.base_uuids_dict = {}

    def init_config(self, config):
        self.session_class = init_db_session_class(config)

    def get_dtable_server_api_by_name(self, name, with_check_upgrade=True) -> DTableServerAPI:
        if with_check_upgrade and not ENABLE_SYSTEM_BASES:
            return None

        dtable_uuid = self.base_uuids_dict.get(name)
        if not dtable_uuid:
            with self.session_class() as session:
                sql = '''
                SELECT uuid FROM dtables d
                JOIN workspaces w ON d.workspace_id=w.id
                WHERE w.owner=:owner AND d.name=:name LIMIT 1
                '''
                results = session.execute(sql, {
                    'owner': self.owner,
                    'name': name
                })
                for item in results:
                    dtable_uuid = uuid_str_to_36_chars(item.uuid)
                if not dtable_uuid:
                    return None
                self.base_uuids_dict[name] = dtable_uuid
        return DTableServerAPI('dtable-events', dtable_uuid, dtable_server_url)

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
        version_dtable_server_api = self.get_dtable_server_api_by_name('version', with_check_upgrade=False)
        if not version_dtable_server_api:
            return '0.0.0'
        sql = f"SELECT version FROM `{self.version_table_name}` LIMIT 1"
        version_dtable_db_api = DTableDBAPI('dtable-events', version_dtable_server_api.dtable_uuid, INNER_DTABLE_DB_URL)
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
        logger.debug('v1s: %s v2s: %s', v1s, v2s)
        for i in range(len(v1s)):
            if int(v1s[i]) > int(v2s[i]):
                return True
            elif int(v1s[i]) < int(v2s[i]):
                return False
        return False

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
        logger.info('current_version: %s', self.current_version)
        for version in self.versions:
            logger.debug('comp version: %s current_version: %s', version, self.current_version)
            need_upgrade = self.comp(version, self.current_version)
            logger.debug('version: %s not need upgrade', version)
            if not need_upgrade:
                logger.info('version %s has been upgraded!', version)
                continue
            self.upgrade_version(version)

        logger.info('all upgrades done!')
        self.is_upgrade_done = True

    def upgrade(self):
        if ENABLE_SYSTEM_BASES:
            threading.Thread(target=self._upgrade, daemon=True).start()


system_bases_manager = SystemBasesManager()
