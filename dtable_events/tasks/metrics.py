import datetime
import logging
import time
from threading import Thread, Event
import json

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.event_redis import redis_cache, REDIS_METRIC_KEY, RedisClient
from dtable_events.utils.utils_metric import METRIC_CHANNEL_NAME

local_metric = {
  'metrics': {}
}

class MetricReceiver(Thread):
    """
    collect metrics from redis channel and save to local
    """
    def __init__(self, config):
        Thread.__init__(self)
        self._finished = Event()
        self._redis_client = RedisClient(config=config)
    def run(self):
        if not self._redis_client.connection:
            logging.warning('Redis connection is not established.')
            return
        subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)
        while not self._finished.is_set():
            try:
                message = subscriber.get_message()
                if message:
                    metric_data = json.loads(message['data'])
                    try:
                        component_name = metric_data.get('component_name')
                        node_name = metric_data.get('node_name', 'default')
                        metric_name = metric_data.get('metric_name')
                        key_name = f"{component_name}:{node_name}:{metric_name}"
                        metric_details = metric_data.get('details') or {}
                        metric_details['metric_value'] = metric_data.get('metric_value')
                        metric_details['metric_type'] = metric_data.get('metric_type')
                        metric_details['metric_help'] = metric_data.get('metric_help')
                        local_metric['metrics'][key_name] = metric_details
                    except Exception as e:
                        logging.error('Error when handling metric data: %s' % e)
                else:
                    time.sleep(0.5)
            except Exception as e:
                logging.error('Failed handle metric %s' % e)
                subscriber = self._redis_client.get_subscriber(METRIC_CHANNEL_NAME)


class MetricSaver(Thread):
    """
    save metrics to redis
    """
    def __init__(self):
        super(MetricSaver, self).__init__()
    def run(self):
        schedule = BlockingScheduler()
        @schedule.scheduled_job('interval', seconds=15, misfire_grace_time=30)
        def time_job():
            try:
                if local_metric['metrics']:
                    logging.info('Start saving metrics... ')
                    for key, metric_detail in local_metric.get('metrics').items():
                        metric_detail['collected_at'] = datetime.datetime.now().isoformat()
                    redis_cache.create_or_update(REDIS_METRIC_KEY, local_metric.get('metrics'))
                    local_metric['metrics'].clear()
            except Exception as e:
                logging.error('metric collect error: %s' % e)
                
        schedule.start()


class MetricManager(object):
    def __init__(self, config):
        self.config = config
    
    def start(self):
        logging.info('Start metric manager...')
        try:
            self._metric_collect_thread = MetricSaver()
            self._metric_collect_thread.start()
        except Exception as e:  
            logging.error('Failed to start metric collect thread: %s' % e)
        try:
            self._metric_task = MetricReceiver(self.config)
            self._metric_task.start()
        except Exception as e:  
            logging.error('Failed to start metric handle thread: %s' % e)
