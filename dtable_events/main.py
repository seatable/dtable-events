# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import argparse
import logging

from dtable_events.app.app import App
from dtable_events.app.log import LogConfigurator
from dtable_events.app.config import get_config, is_syslog_enabled, get_task_mode
from dtable_events.app.event_redis import redis_cache
from dtable_events.db import create_db_tables


def main():
    args = parser.parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile)

    config = get_config(args.config_file)

    redis_cache.init_redis(config)  # init redis instance for redis_cache

    try:
        create_db_tables(config)
    except Exception as e:
        logging.error('Failed create tables, error: %s' % e)
        raise RuntimeError('Failed create tables, error: %s' % e)

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()
 
    task_mode = get_task_mode(args.taskmode)

    app = App(config, task_mode)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('--taskmode', default='all', help='task mode')

    main()
