import os

from kombu import Exchange, Queue

# Broker 和 Backend 配置
broker_url = os.getenv('BROKER_URL')   # RabbitMQ

# 队列定义
task_queues = (
    Queue('trigger_automation_rule', Exchange('default', type='direct'), routing_key='trigger_automation_rule'),
)

# 路由规则
task_routes = {
    'dtable_events.celery_app.tasks.automation_rules.trigger_automation_rule': {'queue': 'trigger_automation_rule'},
}

# timezone
timezone = 'Asia/Shanghai'

# worker 行为配置（防丢失）
task_acks_late = True
task_reject_on_worker_lost = True
worker_prefetch_multiplier = 1
