import os
import json
from dtable_events.app.event_redis import redis_cache

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
