import datetime
import logging
import os
import time
from threading import Thread, Event
import json

from app.event_redis import redis_cache, REDIS_METRIC_KEY, RedisClient


local_metric = {
  'metrics': {}
}

NODE_NAME = os.environ.get('NODE_NAME', 'default')
METRIC_CHANNEL_NAME = "metric_channel"


def handle_metric_timing(metric_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            publish_metric = {
                "metric_name": metric_name,
                "metric_type": "gauge",
                "metric_help": "",
                "component_name": "dtable-events",
                "node_name": NODE_NAME,
                "details": {}
            }
            start_time = time.time()
            func(*args, **kwargs)
            end_time = time.time()
            duration_seconds = end_time - start_time
            publish_metric['metric_value'] = round(duration_seconds, 3)
            redis_cache.publish(METRIC_CHANNEL_NAME, publish_metric)
        return wrapper
    return decorator



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
    def __init__(self, interval, config):
        Thread.__init__(self)
        self._finished = Event()
        self.__interval = interval
        self.config = config
    def run(self):
        while not self._finished.is_set():
            self._finished.wait(self.__interval)
            if not self._finished.is_set():
                try:
                    if local_metric['metrics']:
                        for key, metric_detail in local_metric.get('metrics').items():
                            metric_detail['collected_at'] = datetime.datetime.now().isoformat()
                        redis_cache.init_redis(self.config)
                        redis_cache.create_or_update(REDIS_METRIC_KEY, local_metric.get('metrics'))
                except Exception as e:
                    logging.error('metric collect error: %s' % e)
    def cancel(self):
        self._finished.set()
        logging.info('MetricSaver thread is cancelled.')


class MetricManager(object):
    def __init__(self, config):
        self._interval = 15
        self.config = config
    
    def start(self):
        try:
            self._metric_collect_thread = MetricSaver(self._interval, self.config)
            self._metric_collect_thread.start()
        except Exception as e:  
            logging.error('Failed to start metric collect thread: %s' % e)
        try:
            logging.info('Starting metric handle')
            self._metric_task = MetricReceiver(self.config)
            self._metric_task.start()
        except Exception as e:  
            logging.error('Failed to start metric handle thread: %s' % e)
