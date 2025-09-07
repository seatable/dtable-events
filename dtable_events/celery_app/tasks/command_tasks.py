import logging
import os

from dtable_events.app.event_redis import redis_cache
from dtable_events.celery_app.app import app
from dtable_events.app.config import dtable_web_dir
from dtable_events.utils import get_python_executable, run

logger = logging.getLogger(__name__)


@app.task
def send_instant_notices():
    log_dir = os.path.join(os.environ.get('LOG_DIR', ''))
    logfile = os.path.join(log_dir, 'instant_notice_sender.log')
    lock_key = 'send_instant_notices'
    lock = redis_cache._redis_client.connection.lock(lock_key)
    if lock.acquire(blocking=False):
        try:
            python_exec = get_python_executable()
            manage_py = os.path.join(dtable_web_dir, 'manage.py')
            cmd = [
                python_exec,
                manage_py,
                'send_instant_notices',
            ]

            with open(logfile, 'a') as fp:
                run(cmd, cwd=dtable_web_dir, output=fp)
        except Exception as e:
            logger.exception('send instant notices error: %s', e)
        finally:
            lock.release()
    else:
        logger.info('last send_instant_notices not completed')
