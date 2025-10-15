import os
import json
import time

from dtable_events.app.event_redis import redis_cache
from dtable_events.app.prometheus_client import common_dataset_sync_count_gauge, common_dataset_sync_total_row_count_gauge, \
    common_dataset_sync_time_cost_gauge

def publish_common_dataset_metric_decorator(func):
    def wrapper(*args, **kwargs):
        start_ts = time.monotonic()
        result = func(*args, **kwargs)
        elapsed = round(time.monotonic() - start_ts, 5)
        total_row_count, sync_count = result

        common_dataset_sync_count_gauge.set(sync_count)
        common_dataset_sync_total_row_count_gauge.set(total_row_count)
        common_dataset_sync_time_cost_gauge.set(elapsed)

        return result
    return wrapper
