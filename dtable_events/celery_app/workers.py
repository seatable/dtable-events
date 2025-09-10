import logging
import time
from dataclasses import dataclass
from multiprocessing import Process

from dtable_events.celery_app.app import app

logger = logging.getLogger(__name__)


@dataclass
class WorkersConfig:
    # all workers concurrency config
    trigger_automation_rule: 1
    scan_automation_rules: 1
    send_instant_notices: 1
    # beat config
    enable_beat_scheduler: True



class CeleryWorkers:

    def __init__(self, config):
        section_name = 'CELERY'

        trigger_automation_rule = config.getint(section_name, 'trigger_automation_rule', fallback=1)
        scan_automation_rules = config.getint(section_name, 'scan_automation_rules', fallback=1)
        send_instant_notices = config.getint(section_name, 'send_instant_notices', fallback=1)
        enable_beat_scheduler = config.getboolean(section_name, 'enable_beat_scheduler', fallback=True)

        self.workers_config = WorkersConfig(
            trigger_automation_rule=trigger_automation_rule,
            scan_automation_rules=scan_automation_rules,
            send_instant_notices=send_instant_notices,
            enable_beat_scheduler=enable_beat_scheduler
        )

    def start(self):
        workers = []
        if self.workers_config.trigger_automation_rule > 0:
            workers.append({
                'queue': 'trigger_automation_rule',
                'concurrency': self.workers_config.trigger_automation_rule
            })
        if self.workers_config.scan_automation_rules > 0:
            workers.append({
                'queue': 'scan_automation_rules',
                'concurrency': self.workers_config.scan_automation_rules
            })
        if self.workers_config.send_instant_notices > 0:
            workers.append({
                'queue': 'send_instant_notices',
                'concurrency': self.workers_config.send_instant_notices
            })

        for worker in workers:
            Process(
                target=app.worker_main,
                args=(
                    [
                        "worker",
                        f"--loglevel={logging.getLevelName(logging.root.level)}",
                        "-Q",
                        worker['queue'],
                        "--concurrency",
                        f"{worker['concurrency']}",
                    ],
                ),
                daemon=True
            ).start()
            logger.info('Celery worker %s start', worker)
            time.sleep(1)

        if self.workers_config.enable_beat_scheduler:
            Process(target=app.Beat().run, daemon=True).start()
            logger.info('Celery beat start')
        else:
            logger.info('No celery beat start')
