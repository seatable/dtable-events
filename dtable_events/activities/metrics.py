import datetime
import logging
import os
import time
from threading import Thread, Event
import json

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.event_redis import redis_cache, REDIS_METRIC_KEY, RedisClient


local_metric = {
  'metrics': {}
}

NODE_NAME = os.environ.get('NODE_NAME', 'default')
METRIC_CHANNEL_NAME = "metric_channel"
TASK_MANAGER_METRIC_HELP = "The size of the task queue"
BIG_DATA_TASK_MANAGER_METRIC_HELP = "The size of the big data task queue"
DATA_SYNC_TASK_MANAGER_METRIC_HELP = "The size of the data sync task queue"
MESSAGE_TASK_MANAGER_METRIC_HELP = "The size of the message task queue"
PLUGIN_EMAIL_TASK_MANAGER_METRIC_HELP = "The size of the plugin email task queue"


def publish_io_qsize_metric(qsize, metric_name, metric_help):
    publish_metric = {
        "metric_name": metric_name,
        "metric_type": "gauge",
        "metric_help": metric_help,
        "component_name": 'dtable-events',
        "node_name": NODE_NAME,
        "metric_value": qsize,
        "details": {}
    }
    redis_cache.publish(METRIC_CHANNEL_NAME, json.dumps(publish_metric))


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
    def __init__(self, config):
        super(MetricSaver, self).__init__()
        self.config = config
    def run(self):
        schedule = BlockingScheduler()
        @schedule.scheduled_job('interval', seconds=15, misfire_grace_time=30)
        def time_job():
            try:
                if local_metric['metrics']:
                    for key, metric_detail in local_metric.get('metrics').items():
                        metric_detail['collected_at'] = datetime.datetime.now().isoformat()
                    redis_cache.create_or_update(REDIS_METRIC_KEY, local_metric.get('metrics'))
            except Exception as e:
                logging.error('metric collect error: %s' % e)
                
        schedule.start()


class MetricManager(object):
    def __init__(self, config):
        self.config = config
    
    def start(self):
        try:
            self._metric_collect_thread = MetricSaver(self.config)
            self._metric_collect_thread.start()
        except Exception as e:  
            logging.error('Failed to start metric collect thread: %s' % e)
        try:
            logging.info('Starting metric handle')
            self._metric_task = MetricReceiver(self.config)
            self._metric_task.start()
        except Exception as e:  
            logging.error('Failed to start metric handle thread: %s' % e)
