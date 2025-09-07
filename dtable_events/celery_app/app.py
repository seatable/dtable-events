import os
import configparser

from celery import Celery

from dtable_events.db import init_db_session_class

# 创建 Celery 实例
app = Celery('dtable_events.celery_app')

app.config_from_object('dtable_events.celery_app.celeryconfig')

from dtable_events.celery_app.tasks import automation_rules

def init_app_config(config: configparser.ConfigParser):
    init_conf = {}
    if os.environ.get('BROKER_URL'):
        init_conf['BORKER_URL'] = os.environ.get('BROKER_URL')
    elif config.get('celery', 'broker_url', fallback=''):
        init_conf['BORKER_URL'] = config.get('celery', 'broker_url')
    app.conf.update(init_conf)
    # init and bind db session class
    app.db_session_class = init_db_session_class(config)
