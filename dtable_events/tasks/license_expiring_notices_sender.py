import logging
import os
import re
from datetime import date
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler
from dateutil import parser

from seaserv import ccnet_api

from dtable_events.app.config import LICENSE_PATH, DTABLE_WEB_SERVICE_URL, IS_PRO_VERSION
from dtable_events.utils.dtable_web_api import DTableWebAPI


class LicenseExpiringNoticesSender:

    def __init__(self):
        self.days = [30, 15, 7, 6, 5, 4, 3, 2, 1]
        self.license_path = LICENSE_PATH
        self._enabled = IS_PRO_VERSION  # only pro check license

    def start(self):
        if not self._enabled:
            return
        timer = LicenseExpiringNoticesSenderTimer(self.days, self.license_path)
        logging.info('Start license notices sender...')
        timer.start()


class LicenseExpiringNoticesSenderTimer(Thread):

    def __init__(self, days, license_path):
        super(LicenseExpiringNoticesSenderTimer, self).__init__()
        self.daemon = True
        self.days = days
        self.license_path = license_path

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
                    logging.debug('line: %s', line)
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
            logging.info('license will expire in %s days', days)
            if days not in self.days:
                return
            try:
                admin_users = ccnet_api.get_superusers()
                dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
                to_users = [user.email for user in admin_users]
                dtable_web_api.internal_add_notification(to_users, 'license_expiring', {'days': days})
            except Exception as e:
                logging.exception('send license expiring days: %s to users: %s error: %s', days, to_users, e)

        sched.start()
