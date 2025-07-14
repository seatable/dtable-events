import os
import json
import time
from dtable_events.app.event_redis import redis_cache

NODE_NAME = os.environ.get('NODE_NAME', 'default')
METRIC_CHANNEL_NAME = "metric_channel"
TASK_MANAGER_METRIC_HELP = "The size of the task queue"
BIG_DATA_TASK_MANAGER_METRIC_HELP = "The size of the big data task queue"
DATA_SYNC_TASK_MANAGER_METRIC_HELP = "The size of the data sync task queue"
MESSAGE_TASK_MANAGER_METRIC_HELP = "The size of the message task queue"
PLUGIN_EMAIL_TASK_MANAGER_METRIC_HELP = "The size of the plugin email task queue"
COMMON_DATASET_TOTAL_ROW_COUNT_METRIC_HELP = "Total rows processed in common-dataset sync"
COMMON_DATASET_OPERATIONS_COUNT_METRIC_HELP = "Common-dataset sync count"
COMMON_DATASET_ELAPSED_TIME_METRIC_HELP = "Time (seconds) to complete common-dataset sync job"


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


def publish_common_dataset_metric(value, metric_name, metric_help):
    publish_metric = {
        "metric_name": metric_name,
        "metric_type": "gauge",
        "metric_help": metric_help,
        "component_name": 'dtable-events',
        "node_name": NODE_NAME,
        "metric_value": value,
        "details": {}
    }
    redis_cache.publish(METRIC_CHANNEL_NAME, json.dumps(publish_metric))


def publish_common_dataset_metric_decorator(func):
    def wrapper(*args, **kwargs):
        start_ts = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = round(time.monotonic() - start_ts, 5)
        try:
            total_row_count, sync_count = result
        except Exception:
            total_row_count, sync_count = None, None
        publish_common_dataset_metric(total_row_count, 'common_dataset_sync_total_row_count', COMMON_DATASET_TOTAL_ROW_COUNT_METRIC_HELP)
        publish_common_dataset_metric(sync_count, 'common_dataset_sync_count', COMMON_DATASET_OPERATIONS_COUNT_METRIC_HELP)
        publish_common_dataset_metric(elapsed, 'common_dataset_sync_elapsed_time', COMMON_DATASET_ELAPSED_TIME_METRIC_HELP)
        return result
    return wrapper
