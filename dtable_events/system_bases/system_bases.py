import logging
import time
from datetime import datetime

from seaserv import seafile_api

from dtable_events.app.config import INNER_DTABLE_DB_URL, SYSTEM_BASES_OWNER, ENABLE_SYSTEM_BASES
from dtable_events.db import init_db_session_class
from dtable_events.system_bases.constants import VERSION_BASE_NAME, VERSION_TABLE_NAME
from dtable_events.system_bases.bases import BasicBase, VersionBase, CDSStatisticsBase
from dtable_events.utils import get_inner_dtable_server_url
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI

logger = logging.getLogger(__name__)
dtable_server_url = get_inner_dtable_server_url()


class SystemBasesManager:

    def __init__(self):
        self.versions = [
            '0.0.1'
        ]
        self.current_version = ''
        self.workspace_id = None
        self.repo_id = ''

        self.base_map = {}

        self.is_ready = False

    def init_config(self, config):
        self.session_class = init_db_session_class(config)
        try:
            self.load_workspace_and_bases()
        except Exception as e:
            logger.exception('load system workspace and bases error: %s', e)

    def create_workspace(self):
        repo_id = seafile_api.create_repo(
            "My Workspace",
            "My Workspace",
            "dtable@seafile"
        )
        now = datetime.now()
        with self.session_class() as session:
            sql = '''
            INSERT INTO workspaces(owner, repo_id, created_at, org_id) VALUES
            (:owner, :repo_id, :created_at, :org_id)
            '''
            result = session.execute(sql, {
                'owner': SYSTEM_BASES_OWNER,
                'repo_id': repo_id,
                'created_at': now,
                'org_id': -1
            })
            session.commit()
            self.workspace_id = result.lastrowid
            self.repo_id = repo_id

    def load_workspace(self):
        with self.session_class() as session:
            sql = "SELECT id, repo_id FROM workspaces WHERE owner=:owner"
            result = session.execute(sql, {'owner': SYSTEM_BASES_OWNER}).fetchone()
            if not result:
                self.create_workspace()
            else:
                self.workspace_id = result.id
                self.repo_id = result.repo_id

    def load_bases(self):
        self.base_map[VersionBase.base_name] = VersionBase(self.session_class, self.workspace_id, self.repo_id)
        self.base_map[CDSStatisticsBase.base_name] = CDSStatisticsBase(self.session_class, self.workspace_id, self.repo_id)

    def load_workspace_and_bases(self):
        self.load_workspace()
        self.load_bases()

    def get_base_by_name(self, name) -> BasicBase:
        if self.is_ready and ENABLE_SYSTEM_BASES:
            return None
        base = self.base_map.get(name)
        if not base:
            return None
        if not base.is_ready:
            return None

    def request_current_version(self):
        """
        :return: current_version -> str or None: None means version base invalid
        """
        version_base = self.base_map[VERSION_BASE_NAME]
        sql = f"SELECT version FROM `{VERSION_TABLE_NAME}` LIMIT 1"
        try:
            if not version_base.check():
                if not version_base.dtable_uuid:
                    logger.info('version base not exists, create one')
                    version_base.create()
                    return '0.0.0'
                else:
                    logger.error('version base exists in database but base invalid')
                    return None
        except Exception as e:
            logger.exception('check or create version base error: %s', e)
            return None
        version_dtable_db_api = version_base.get_dtable_db_api()
        try:
            results, _ = version_dtable_db_api.query(sql, convert=True, server_only=True)
        except Exception as e:
            logger.error('query version error: %s', e)
            return None
        if not results:
            return '0.0.0'
        return results[0]['version']

    @staticmethod
    def comp(v1, v2) -> bool:
        """
        :return: v1 > v2
        """
        v1 = tuple(int(v) for v in v1.split('.'))
        v2 = tuple(int(v) for v in v2.split('.'))

    def update_version(self, version):
        self.base_map[VERSION_BASE_NAME].get_dtable_server_api().append_row(VERSION_TABLE_NAME, {'version': version})

    def _upgrade(self):
        if not self.workspace_id:
            logger.error('workspace invalid')
            return

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
        logger.info('system bases current version: %s', self.current_version)

        logger.info('start to upgrade versions: %s', self.versions)
        # upgrade and re-create non-exist bases
        for version in self.versions:
            logger.info('start to upgrade version: %s', version)
            if self.comp(version, self.current_version):
                logger.info('version: %s need to upgrade', version)
            else:
                logger.info('version: %s not need to upgrade', version)
                continue
            upgrade_method_name = f"upgrade_{version.replace('.', '_')}"
            for base in self.base_map.values():
                if base.base_name == VERSION_BASE_NAME:
                    continue
                logger.info('start to upgrade base: %s version: %s', base.base_name, version)
                if base.check_db_base():
                    if hasattr(base, upgrade_method_name):
                        getattr(base, upgrade_method_name)()
                        logger.info('base: %s version: %s upgraded', base.base_name, version)
                    else:
                        logger.info('base: %s not need to upgrade to %s', base.base_name, version)
            self.update_version(version)

        # start to create non-exist bases
        logger.info('start to scan non-exist bases')
        for base in self.base_map.values():
            if base.base_name == VERSION_BASE_NAME:
                continue
            if base.check_db_base():
                if base.check_table():
                    base.is_ready = True
                continue
            logger.info('base: %s not found start to create...', base.base_name)
            base.create()
            logger.info('base: %s created', base.base_name)
        logger.info('scan non-exist bases and create finish')

        self.is_ready = True

    def upgrade(self):
        try:
            self._upgrade()
        except Exception as e:
            logger.exception('system bases upgrade error: %s', e)


system_bases_manager = SystemBasesManager()
