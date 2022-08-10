# -*- coding: utf-8 -*-
import os
import logging
from threading import Thread, Event

from dtable_events.utils import get_opt_from_conf_or_env, parse_bool, get_python_executable, run, parse_interval
from dtable_events.app.config import dtable_web_dir

__all__ = [
    'EmailNoticesSender',
]


class EmailNoticesSender(object):
    def __init__(self, config):
        self._enabled = True
        self._logfile = None
        self._interval = 60 * 60  # 60min
        self._prepare_logfile()
        self._parse_config(config)

    def _prepare_logfile(self):
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'email_notices_sender.log')

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'EMAIL SENDER'
        key_enabled = 'enabled'
        key_interval = 'interval'

        if not config.has_section(section_name):
            return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled
        # interval
        interval = get_opt_from_conf_or_env(config, section_name, key_interval, default=60 * 60)
        interval = parse_interval(interval, 60 * 60)
        self._interval = interval

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start email notices sender: it is not enabled!')
            return

        logging.info('Start email notices sender, interval = %s sec', self._interval)

        SendSeahubEmailTimer(self._interval, self._logfile).start()

    def is_enabled(self):
        return self._enabled


class SendSeahubEmailTimer(Thread):

    def __init__(self, interval, logfile):
        Thread.__init__(self)
        self._interval = interval
        self._logfile = logfile
        self.finished = Event()

    def run(self):
        while not self.finished.is_set():
            self.finished.wait(self._interval)
            if not self.finished.is_set():
                logging.info('Starts to send email...')
                try:
                    python_exec = get_python_executable()
                    manage_py = os.path.join(dtable_web_dir, 'manage.py')

                    cmd = [
                        python_exec,
                        manage_py,
                        'send_email_notices',
                    ]
                    with open(self._logfile, 'a') as fp:
                        run(cmd, cwd=dtable_web_dir, output=fp)
                except Exception as e:
                    logging.exception('error when send email: %s', e)

    def cancel(self):
        self.finished.set()
