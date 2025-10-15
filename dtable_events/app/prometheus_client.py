import logging
import threading
import os
import time

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

logger = logging.getLogger(__name__)

registry = CollectorRegistry()

io_task_queue_size_gauge = Gauge(
    'io_task_queue_size',
    'The number of io tasks in the queue',
    labelnames=['node', 'component'],
    registry=registry
)

big_data_io_task_queue_size_gauge = Gauge(
    'big_data_io_task_queue_size',
    'The number of big data tasks in the queue',
    labelnames=['node', 'component'],
    registry=registry
)

message_io_task_queue_size_gauge = Gauge(
    'message_io_task_queue_size',
    'The number of notification message tasks in the queue, including emails',
    labelnames=['node', 'component'],
    registry=registry
)

plugin_email_io_task_queue_size_gauge = Gauge(
    'plugin_email_io_task_queue_size',
    'The number of plugin email tasks in the queue',
    labelnames=['node', 'component'],
    registry=registry
)

realtime_automation_queue_size_gauge = Gauge(
    'realtime_automation_queue_size',
    'The number of triggered realtime automations in the queue',
    labelnames=['node', 'component'],
    registry=registry
)

realtime_automation_heartbeat_gauge = Gauge(
    'realtime_automation_heartbeat',
    'The heartbeat of realtime automations',
    labelnames=['node', 'component'],
    registry=registry
)

scheduled_automation_queue_size_gauge = Gauge(
    'scheduled_automation_queue_size',
    'The number of triggered scheduled automations in the queue',
    labelnames=['node', 'component'],
    registry=registry
)

common_dataset_sync_count_gauge = Gauge(
    'common_dataset_sync_count',
    'Common-dataset syncs count',
    labelnames=['node', 'component'],
    registry=registry
)

common_dataset_sync_total_row_count_gauge = Gauge(
    'common_dataset_sync_total_row_count',
    'Total rows processed in common-dataset syncs',
    labelnames=['node', 'component'],
    registry=registry
)

common_dataset_sync_time_cost_gauge = Gauge(
    'common_dataset_sync_time_cost',
    'Time taken (in seconds) to complete common-dataset syncs job',
    labelnames=['node', 'component'],
    registry=registry
)

# pre defined labels
io_task_queue_size_gauge = io_task_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
big_data_io_task_queue_size_gauge = big_data_io_task_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
message_io_task_queue_size_gauge = message_io_task_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
plugin_email_io_task_queue_size_gauge = plugin_email_io_task_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
realtime_automation_queue_size_gauge = realtime_automation_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
realtime_automation_heartbeat_gauge = realtime_automation_heartbeat_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
scheduled_automation_queue_size_gauge = scheduled_automation_queue_size_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
common_dataset_sync_count_gauge = common_dataset_sync_count_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
common_dataset_sync_total_row_count_gauge = common_dataset_sync_total_row_count_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')
common_dataset_sync_time_cost_gauge = common_dataset_sync_time_cost_gauge.labels(node=os.environ.get('NODE_NAME'), component='dtable-events')


class MetricPusher:

    def __init__(self, config):
        self.config = config
        self.pushgateway_url = ''
        self.job_name = ''
        self.interval = 5
        self.enabled = False
        self.parse_config()

    def parse_config(self):
        self.enabled = self.config.getboolean('PUSHGATEWAY', 'enabled', fallback=False)
        self.pushgateway_url = self.config.get('PUSHGATEWAY', 'url')
        self.job_name = self.config.get('PUSHGATEWAY', 'job_name')
        print(f'self.pushgateway_url: {self.pushgateway_url}')

    def push_loop(self):
        while True:
            try:
                push_to_gateway(
                    self.pushgateway_url,
                    job=self.job_name,
                    registry=registry
                )
                logger.info("Metrics pushed immediately")
            except Exception as e:
                logger.error(f"Failed to push metrics immediately: {e}")
            time.sleep(self.interval)

    def start(self):
        if not self.enabled:
            logger.warning(f"Can not start metric pusher: it is not enabled!")
            return
        threading.Thread(target=self.push_loop, daemon=True).start()
