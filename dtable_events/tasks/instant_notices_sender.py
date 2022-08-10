# -*- coding: utf-8 -*-
import os
import sys
import logging
from threading import Thread, Event

from dtable_events.app.config import ENABLE_WEIXIN, ENABLE_WORK_WEIXIN, ENABLE_DINGTALK, dtable_web_dir
from dtable_events.utils import get_python_executable, parse_bool, \
     parse_interval, get_opt_from_conf_or_env, run

__all__ = [
    'InstantNoticeSender',
]


class InstantNoticeSender(object):

    def __init__(self, config):
        self._enabled = False
        self._interval = None
        self._logfile = None
        self._parse_config(config)
        self._prepare_logfile()

    def _prepare_logfile(self):
        log_dir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(log_dir, 'instant_notice_sender.log')

    def _parse_config(self, config):
        """parse instant related options from config file
        """
        section_name = 'INSTANT SENDER'
        key_interval = 'interval'
        default_interval = 60  # 1min

        # enabled
        enabled = ENABLE_WEIXIN or ENABLE_WORK_WEIXIN or ENABLE_DINGTALK
        enabled = parse_bool(enabled)
        if not enabled:
            return
        self._enabled = True

        # notice send interval
        if config.has_section(section_name):
            interval = get_opt_from_conf_or_env(config, section_name, key_interval,
                                                default=default_interval).lower()
            interval = parse_interval(interval, default_interval)
        else:
            interval = default_interval

        self._interval = interval

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start instant notice sender: it is not enabled!')
            return

        logging.info('Start instant notice sender, interval = %s sec', self._interval)

        InstantNoticeSenderTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class InstantNoticeSenderTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                # logging.info('Start to send instant notices..')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')
                    cmd = [
                        python_exec,
                        manage_py,
                        'send_instant_notices',
                    ]

                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('send instant notices error: %s', e)

    def cancel(self):
        self.finished.set()
