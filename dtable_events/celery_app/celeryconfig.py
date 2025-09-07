import os

from celery.schedules import crontab
from kombu import Exchange, Queue

# Broker 和 Backend 配置
broker_url = os.getenv('BROKER_URL')   # RabbitMQ

# 队列定义
task_queues = (
    Queue('trigger_automation_rule', Exchange('default', type='direct'), routing_key='trigger_automation_rule'),
    Queue('scan_automation_rules', Exchange('default', type='direct'), routing_key='scan_automation_rules'),
    Queue('send_instant_notices', Exchange('default', type='direct'), routing_key='send_instant_notices'),
)

# 路由规则
task_routes = {
    # automation rules
    'dtable_events.celery_app.tasks.automation_rules.trigger_automation_rule': {'queue': 'trigger_automation_rule'},
    'dtable_events.celery_app.tasks.automation_rules.scan_automation_rules': {'queue': 'scan_automation_rules'},
    # command tasks
    'dtable_events.celery_app.tasks.command_tasks.send_instant_notices': {'queue': 'send_instant_notices'},
}

beat_schedule = {
    'interval_automation_rules': {
        'task': 'dtable_events.celery_app.tasks.automation_rules.scan_automation_rules',
        'schedule': crontab(minute="0")
    },
    'interval_send_instant_notices': {
        'task': 'dtable_events.celery_app.tasks.command_tasks.send_instant_notices',
        'schedule': crontab(minute="*")
    }
}


# timezone
timezone = 'Asia/Shanghai'

# worker 行为配置（防丢失）
task_acks_late = True
task_reject_on_worker_lost = True
worker_prefetch_multiplier = 1
