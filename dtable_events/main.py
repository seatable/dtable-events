# -*- coding: utf-8 -*-
import os
import argparse
import logging

from dtable_events.db import create_db_tables
from dtable_events.app.app import App
from dtable_events.app.log import LogConfigurator
from dtable_events.app.config import get_config, is_syslog_enabled


def main():
    args = parser.parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile)

    if args.logfile:
        log_dir = os.path.dirname(os.path.realpath(args.logfile))
        os.environ['DTABLE_EVENTS_LOG_DIR'] = log_dir

    os.environ['DTABLE_EVENTS_CONFIG_FILE'] = os.path.expanduser(args.config_file)

    config = get_config(args.config_file)
    try:
        create_db_tables(config)
    except Exception as e:
        logging.error('Failed create tables, error:', e)
        raise RuntimeError('Failed create tables.')

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()

    app = App(config)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')

    main()
