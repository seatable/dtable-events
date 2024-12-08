import logging
import time
from threading import Thread

try:
    import psutil
except:
    psutil = None

from dtable_events.app.log import setup_logger

monitor_logger = setup_logger('browser-monitor.log')


class ProcessMonitor(Thread):
    def __init__(self, interval=1):
        super(ProcessMonitor, self).__init__()
        self.pid_infos = dict()
        self.interval = interval  # refresh interval
        self.running = False
        self.daemon = True
        # self.monitor_logger = setup_logger('browser-monitor.log')

    def can_monitor(self):
        return monitor_logger.root.level == logging.DEBUG
        # return self.monitor_logger.root.level == logging.DEBUG

    def add_pid_info(self, pid_info):
        if not self.can_monitor():
            return
        self.pid_infos.update(pid_info)

    def remove_pid(self, pid):
        if not self.can_monitor():
            return
        self.pid_infos.pop(pid, None)

    def run(self):
        if not self.can_monitor():
            monitor_logger.info("No monitoring!")
            return
        monitor_logger.info("Starting monitoring...")
        self.running = True
        while self.running:
            for pid in list(self.pid_infos.keys()):
                total_cpu = 0.0
                total_memory = 0.0
                try:
                    process = psutil.Process(pid)
                    total_cpu += process.cpu_percent(interval=0)
                    total_memory += process.memory_info().rss

                    # stats sub-processes
                    for child in process.children(recursive=True):
                        try:
                            total_cpu += child.cpu_percent(interval=0)
                            total_memory += child.memory_info().rss
                        except psutil.NoSuchProcess:
                            continue

                except psutil.NoSuchProcess:
                    monitor_logger.info(f"pid: {self.pid_infos[pid]} not exists, removed!")
                    self.pid_infos.pop(pid, None)  # remove when pid not exists longer
                    continue

                total_memory_mb = total_memory / (1024 * 1024)  # convert to MB
                monitor_logger.info(f"pid: {self.pid_infos.get(pid)} Total CPU: {total_cpu:.2f}%, Total Memory: {total_memory_mb:.2f} MB")

            time.sleep(self.interval)


browser_monitor = ProcessMonitor()
