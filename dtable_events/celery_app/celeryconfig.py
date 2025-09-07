import os

from celery.schedules import crontab
from kombu import Exchange, Queue

# Broker 和 Backend 配置
broker_url = os.getenv('BROKER_URL')   # RabbitMQ

# 队列定义
task_queues = (
    Queue('trigger_automation_rule', Exchange('default', type='direct'), routing_key='trigger_automation_rule'),
    Queue('scan_automation_rules', Exchange('default', type='direct'), routing_key='scan_automation_rules'),
)

# 路由规则
task_routes = {
    'dtable_events.celery_app.tasks.automation_rules.trigger_automation_rule': {'queue': 'trigger_automation_rule'},
    'dtable_events.celery_app.tasks.automation_rules.scan_automation_rules': {'queue': 'scan_automation_rules'},
}

beat_schedule = {
    'interval_automation_rules': {
        'task': 'dtable_events.celery_app.tasks.automation_rules.scan_automation_rules', # 这里要写全路径，否则worker找不到
        'schedule': crontab(minute="50")
    }
}


# timezone
timezone = 'Asia/Shanghai'

# worker 行为配置（防丢失）
task_acks_late = True
task_reject_on_worker_lost = True
worker_prefetch_multiplier = 1
