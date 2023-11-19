import os
import json
import logging
import time
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import ENABLE_SYSTEM_BASES
from dtable_events.app.event_redis import redis_cache
from dtable_events.system_bases.system_bases import system_bases_manager
from dtable_events.system_bases.constants import CDS_STATISTICS_MSG_KEY, CDS_STATISTICS_BASE_NAME, CDS_STATISTICS_TABLE_NAME
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool

logger = logging.getLogger(__name__)


__all__ = [
    'SystemBasesRecorder',
]


class SystemBasesRecorder(object):

    def __init__(self, config):
        self._enabled = ENABLE_SYSTEM_BASES
        self._parse_config(config)
        self.keys = [CDS_STATISTICS_MSG_KEY]
        self.interval = 5 * 60  # interval in seconds
        self.batch_count = 100

    def _parse_config(self, config):
        section_name = 'SYSTEM-BASES-RECORDER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=ENABLE_SYSTEM_BASES)
        enabled = parse_bool(enabled) and ENABLE_SYSTEM_BASES
        self._enabled = enabled

        key_interval = 'interval'
        interval = get_opt_from_conf_or_env(config, section_name, key_interval, default=5)
        try:
            interval = int(interval) * 60
        except:
            interval = 5 * 60
        self.interval = interval

    def start(self):
        if not self.is_enabled():
            logger.warning('Can not start system bases recorder: it is not enabled!')
            return

        logger.info('Start system bases recorder')

        SystemBasesRecorderTimer(self.keys, self.interval, self.batch_count).start()

    def is_enabled(self):
        return self._enabled
    

class SystemBasesRecorderTimer(Thread):

    def __init__(self, keys, interval, batch_count):
        super(SystemBasesRecorderTimer, self).__init__()
        self.daemon = True
        self.keys = keys
        self.interval = interval
        self.batch_count = batch_count
        self.redis_conn = redis_cache._redis_client.connection

    def record(self, key, msg_list):
        if key == CDS_STATISTICS_MSG_KEY:
            try:
                base = system_bases_manager.get_base_by_name(CDS_STATISTICS_BASE_NAME)
                dtable_db_api = base.get_dtable_db_api()
                try:
                    dtable_db_api.insert_rows(CDS_STATISTICS_TABLE_NAME, msg_list)
                except Exception as e:
                    logger.error('insert to %s error: %s msg_list: %s', CDS_STATISTICS_BASE_NAME, e, msg_list)
            except Exception as e:
                logger.exception('record key: %s error: %s', key, e)

    def run(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('interval', seconds=self.interval, max_instances=1)
        def record():
            if not system_bases_manager.is_ready:
                logger.debug('system bases not enabled...')
            for key in self.keys:
                logger.debug('start to scan %s', key)
                if key == CDS_STATISTICS_MSG_KEY:
                    base = system_bases_manager.get_base_by_name(CDS_STATISTICS_BASE_NAME)
                    if not base:
                        logger.warning('key: %s base not enabled', key)
                        continue
                msg_list = []
                while True:
                    msg_str = self.redis_conn.lpop(key)
                    if not msg_str:
                        break
                    try:
                        msg = json.loads(msg_str)
                    except:
                        continue
                    msg_list.append(msg)
                    logger.debug('key: %s msg_list: %s', key, len(msg_list))
                    if len(msg_list) >= self.batch_count:
                        self.record(key, msg_list)
                        msg_list = []
                if msg_list:
                    self.record(key, msg_list)

        sched.start()
