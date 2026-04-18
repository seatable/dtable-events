# coding: utf-8

import sys
import logging

from dtable_events.db import prepare_seafile_tables
from dtable_events.virus_scanner.scan_settings import Settings
from dtable_events.virus_scanner.virus_scan import VirusScan

from dtable_events.app.config import get_config

if __name__ == "__main__":
    kw = {
        'format': '[%(asctime)s] [%(levelname)s] %(message)s',
        'datefmt': '%m/%d/%Y %H:%M:%S',
        'level': logging.INFO,
        'stream': sys.stdout
    }
    logging.basicConfig(**kw)

    from dtable_events.virus_scanner.scan_settings import logger
    logger.setLevel(logging.INFO)

    setting = Settings()
    if setting.is_enabled():
        prepare_seafile_tables()
        VirusScan(setting).start()
    else:
        logger.info('Virus scan is disabled.')
