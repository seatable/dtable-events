# coding: utf-8

import os
import sys
import logging
import argparse

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

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config-file',
                        default=os.path.join(os.path.abspath('..'), 'dtable-events.conf'),
                        help='dtable-events config file')
    args = parser.parse_args()

    config = get_config(args.config_file)
    seafile_conf_path = '/opt/seafile/conf/seafile.conf'
    for conf_dir in [
        os.environ.get('SEAFILE_CENTRAL_CONF_DIR'),
        os.environ.get('SEAFILE_CONF_DIR')
    ]:
        if os.path.isfile(os.path.join(conf_dir, 'seafile.conf')):
            seafile_conf_path = os.path.join(conf_dir, 'seafile.conf')
            break

    seafile_config = get_config(seafile_conf_path)

    setting = Settings(config, seafile_config)
    if setting.is_enabled():
        prepare_seafile_tables(seafile_config)
        VirusScan(setting).start()
    else:
        logger.info('Virus scan is disabled.')
