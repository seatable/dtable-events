# celery_app/periodic_tasks.py
from celery.schedules import crontab
from dtable_events.celery_app.app import app

# 配置周期性任务（Celery Beat Schedule）
app.conf.beat_schedule = {
    # # 1. 每天凌晨1点发送日报
    # 'send-daily-report-at-1am': {
    #     'task': 'celery_app.tasks.send_daily_report',  # 任务的完整路径
    #     'schedule': crontab(hour=1, minute=0),  # 每天1:00 AM
    #     'args': (),  # 传递给任务的参数
    #     'kwargs': {'report_type': 'daily'},  # 关键字参数
    #     'options': {'queue': 'reports'}  # 额外的执行选项，比如指定队列
    # },
    
    # 2. 每小时扫描自动化规则
    'hourly-scan-automation-rules': {
        'task': 'dtable_events.celery_app.tasks.automation_rules.scan_automation_rules',
        'schedule': crontab(day_of_week='*', hour='*', minute='0'),
        'args': (),
    },
}

# 可选：设置任务的时区（如果与配置中的默认时区不同）
app.conf.timezone = 'Asia/Shanghai'

# 可选：定义任务的优先级（如果你使用了优先级队列）
app.conf.beat_schedule = {
    'high-priority-cache-warmup': {
        'task': 'celery_app.tasks.warmup_cache',
        'schedule': crontab(minute='*/5'),  # 每5分钟
        'options': {
            'queue': 'high_priority',
            'priority': 0,  # 最高优先级
        }
    },
}
