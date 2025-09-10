import os
import configparser

from celery import Celery, signals

from dtable_events.app.config import get_config
from dtable_events.app.event_redis import redis_cache
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
    conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR')
    events_conf_path = os.path.join(conf_dir, 'dtable-events.conf')
    config = get_config(events_conf_path)
    SessionLocal = init_db_session_class(config)
    redis_cache.init_redis(config)

def get_session_class():
    global SessionLocal
    return SessionLocal

from dtable_events.celery_app.tasks import automation_rules
from dtable_events.celery_app.tasks import command_tasks
