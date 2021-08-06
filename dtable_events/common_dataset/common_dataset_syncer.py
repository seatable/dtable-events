import os
import logging
from threading import Thread, Event

from dtable_events import init_db_session_class
from dtable_events.common_dataset.common_dataset_utils import check_common_dataset
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, parse_interval


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist')


class CommonDatasetSyncer(object):

    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60
        self._prepare_logfile()
        self._prepara_config(config)
        self._db_session_class = init_db_session_class(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'common_dataset_syncer.log')

    def _prepara_config(self, config):
        section_name = 'COMMON_DATASET_SYNC'
        key_enabled = 'enabled'
        key_sync_interval = 'sync_interval'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=False)
        self._enabled = parse_bool(enabled)
        interval = get_opt_from_conf_or_env(config, section_name, key_sync_interval, default=60*60)
        self._interval = parse_interval(interval, 60*60)

    def start(self):
        if not self.is_enabled():
            logging.warning('Common dataset syncer not enabled')
            return
        logging.info('Start common dataset syncer')
        CommonDatasetSyncerTimer(self._interval, self._logfile, self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


class CommonDatasetSyncerTimer(Thread):
    def __init__(self, interval, logfile, db_session_class):
        super(CommonDatasetSyncerTimer, self).__init__()
        self._interval = interval
        self._logfile = logfile
        self.db_session_class = db_session_class
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to common dataset sync...')
                db_session = self.db_session_class()
                try:
                    check_common_dataset(db_session)
                except Exception as e:
                    logging.exception('error when sync common dataset: %s', e)

    def cancel(self):
        self.finished.set()
