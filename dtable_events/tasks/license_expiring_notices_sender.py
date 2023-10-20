import logging
import os
import re
from datetime import date
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil import parser

from seaserv import ccnet_api

from dtable_events.app.config import LICENSE_PATH, DTABLE_WEB_SERVICE_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env
from dtable_events.utils.dtable_web_api import DTableWebAPI


class LicenseExpiringNoticesSender:

    def __init__(self, config):
        self._db_session_class = init_db_session_class(config)
        self.days = [35, 15, 7, 6, 5, 4, 3, 2, 1]
        self.license_path = LICENSE_PATH
        self._parse_config(config)
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        self._logfile = os.path.join(logdir, 'license_expiring_notices_sender.log')

    def _parse_config(self, config):
        section_name = 'LICENSE-EXPIRING-NOTICES-SENDER'
        key_days = 'days'
        if not config.has_section(section_name):
            return
        
        days_str = get_opt_from_conf_or_env(config, section_name, key_days, default='30,15,7,6,5,4,3,2,1')
        days = []
        for day_str in days_str.split(','):
            try:
                day = int(day_str.strip())
                if day > 0 and day not in days:
                    days.append(day)
            except:
                pass
        if days:
            self.days = days

    def start(self):
        timer = LicenseExpiringNoticesSenderTimer(self._db_session_class, self.days, self.license_path, self._logfile)
        logging.info('Start license notices sender...')
        timer.start()


class LicenseExpiringNoticesSenderTimer(Thread):

    def __init__(self, db_session_class, days, license_path, _log_file):
        super(LicenseExpiringNoticesSenderTimer, self).__init__()
        self.daemon = True
        self.db_session_class = db_session_class
        self.days = days
        self.license_path = license_path
        self._logfile = _log_file

    def run(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='7')
        def check():
            logging.info('start to check license...')
            if not os.path.isfile(self.license_path):
                logging.warning('No license file found')
                return
            expire_str = ''
            with open(self.license_path, 'r') as f:
                for line in f.readlines():
                    line = line.strip()
                    logging.info('line: %s', line)
                    if line.startswith('Expiration'):
                        expire_str = line
                        break
            if not expire_str:
                logging.warning('No license expiration found')
                return
            date_strs = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', expire_str)
            if not date_strs:
                logging.warning('No expire date found: %s', expire_str)
                return
            try:
                expire_date = parser.parse(date_strs[0]).date()
            except Exception as e:
                logging.warning('No expire date found: %s error: %s', expire_str, e)
                return
            days = (expire_date - date.today()).days
            if days not in self.days:
                return
            admin_users = ccnet_api.get_superusers()
            dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
            to_users = [user.email for user in admin_users]
            try:
                dtable_web_api.internal_add_notification(to_users, 'license_expiring', {'days': days})
            except Exception as e:
                logging.exception('send license expiring days: %s to users: %s error: %s', days, to_users, e)

        sched.start()
