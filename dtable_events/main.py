# -*- coding: utf-8 -*-
import argparse
import logging
import os

from dtable_events.app.app import App
from dtable_events.app.log import LogConfigurator
from dtable_events.app.config import get_config, is_syslog_enabled, get_task_mode
from dtable_events.app.event_redis import redis_cache
from dtable_events.db import create_db_tables, prepare_seafile_tables


def main():
    args, _ = parser.parse_known_args()
    LogConfigurator()

    redis_cache.init_redis()  # init redis instance for redis_cache

    try:
        create_db_tables()
        prepare_seafile_tables()
    except Exception as e:
        logging.error('Failed create or prepare tables, error: %s' % e)
        raise RuntimeError('Failed create or prepare tables, error: %s' % e)

    task_mode = get_task_mode(args.taskmode)

    app = App(task_mode)
    app.serve_forever()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--taskmode', default='all', help='task mode')

    main()
