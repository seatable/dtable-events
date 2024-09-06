# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import argparse
import logging
import os

from dtable_events.app.app import App
from dtable_events.app.log import LogConfigurator
from dtable_events.app.config import get_config, is_syslog_enabled, get_task_mode
from dtable_events.app.event_redis import redis_cache
from dtable_events.db import create_db_tables, prepare_seafile_tables


def main():
    args = parser.parse_args()
    app_logger = LogConfigurator(args.loglevel, args.logfile)

    config = get_config(args.config_file)

    if is_syslog_enabled(config):
        app_logger.add_syslog_handler()

    redis_cache.init_redis(config)  # init redis instance for redis_cache

    seafile_conf_path = '/opt/seafile/conf/seafile.conf'
    for conf_dir in [
        os.environ.get('SEAFILE_CENTRAL_CONF_DIR'),
        os.environ.get('SEAFILE_CONF_DIR')
    ]:
        if os.path.isfile(os.path.join(conf_dir, 'seafile.conf')):
            seafile_conf_path = os.path.join(conf_dir, 'seafile.conf')
            break

    seafile_config = get_config(seafile_conf_path)

    try:
        create_db_tables(config)
        prepare_seafile_tables(seafile_config)
    except Exception as e:
        logging.error('Failed create or prepare tables, error: %s' % e)
        raise RuntimeError('Failed create or prepare tables, error: %s' % e)

    task_mode = get_task_mode(args.taskmode)

    app = App(config, seafile_config, task_mode)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', help='config file')
    parser.add_argument('--logfile', help='log file')
    parser.add_argument('--loglevel', default='info', help='log level')
    parser.add_argument('--taskmode', default='all', help='task mode')

    main()
