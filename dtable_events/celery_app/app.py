import os
import configparser

from celery import Celery, signals

from dtable_events.db import init_db_session_class

# 创建 Celery 实例
app = Celery('dtable_events.celery_app')

app.config_from_object('dtable_events.celery_app.celeryconfig')

def init_app_config(config: configparser.ConfigParser):
    init_conf = {'config': config}
    if os.environ.get('BROKER_URL'):
        init_conf['BORKER_URL'] = os.environ.get('BROKER_URL')
    elif config.get('celery', 'broker_url', fallback=''):
        init_conf['BORKER_URL'] = config.get('celery', 'broker_url')
    app.conf.update(init_conf)

SessionLocal = None
@signals.worker_process_init.connect
def setup_worker_session_class(*args, **kwargs):
    global SessionLocal
    SessionLocal = init_db_session_class(app.conf.config)

def get_session_class():
    global SessionLocal
    return SessionLocal

from dtable_events.celery_app.tasks import automation_rules
from dtable_events.celery_app.tasks import command_tasks
