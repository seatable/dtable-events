# -*- coding: utf-8 -*-
import os
import argparse
import logging
import json

from dtable_events.db import create_db_tables
from dtable_events.app.app import App
from dtable_events.app.log import LogConfigurator
from dtable_events.app.config import get_config, is_syslog_enabled, get_task_mode


def main():
    args = parser.parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile)

    dtable_server_config_path = os.environ['DTABLE_SERVER_CONFIG']
    with open(dtable_server_config_path) as f:
        dtable_server_config = json.load(f)

    config = get_config(args.config_file)
    try:
        create_db_tables(config)
    except Exception as e:
        logging.error('Failed create tables, error: %s' % e)
        raise RuntimeError('Failed create tables, error: %s' % e)

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()
 
    task_mode = get_task_mode(args.taskmode)

    app = App(config, dtable_server_config, task_mode)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('--taskmode', default='all', help='task mode')

    main()
