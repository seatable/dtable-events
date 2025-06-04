import json

def publish_io_qsize_metric(qsize, metric_name):
        from dtable_events.app.event_redis import redis_cache
        from dtable_events.activities.metrics import NODE_NAME, METRIC_CHANNEL_NAME
        publish_metric = {
            "metric_name": metric_name,
            "metric_type": "gauge",
            "metric_help": "The size of the io task queue",
            "component_name": 'dtable-events',
            "node_name": NODE_NAME,
            "metric_value": qsize,
            "details": {}
        }
        redis_cache.publish(METRIC_CHANNEL_NAME, json.dumps(publish_metric))
