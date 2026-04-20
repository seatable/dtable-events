# coding: utf-8
import os
import logging

from dtable_events.app.config import VIRUS_SCAN_ENABLED, VIRUS_SCAN_SCAN_COMMAND, VIRUS_SCAN_VIRUS_CODE, \
    VIRUS_SCAN_NONVIRUS_CODE, VIRUS_SCAN_SCAN_INTERVAL, VIRUS_SCAN_SCAN_SIZE_LIMIT, VIRUS_SCAN_SCAN_SKIP_EXT, \
    VIRUS_SCAN_THREADS, EMAIL_SENDER_ENABLED
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool
from dtable_events.db import init_db_session_class, init_seafile_db_session_class

logger = logging.getLogger('virus_scan')
logger.setLevel(logging.INFO)


class Settings(object):
    def __init__(self):
        self.enable_scan = False
        self.scan_cmd = None
        self.vir_codes = None
        self.nonvir_codes = None
        self.scan_interval = 60
        # default 20M
        self.scan_size_limit = 20
        self.scan_skip_ext = ['.bmp', '.gif', '.ico', '.png', '.jpg',
                              '.mp3', '.mp4', '.wav', '.avi', '.rmvb',
                              '.mkv']
        self.threads = 4
        self.enable_send_mail = False

        self.session_cls = None
        self.seaf_session_cls = None

        self.parse_config()

    def parse_config(self):
        if not self.parse_scan_config():
            return

        try:
            self.session_cls = init_db_session_class()
            self.seaf_session_cls = init_seafile_db_session_class()
        except Exception as e:
            logger.warning('Failed to init db session class: %s', e)
            return

        self.enable_scan = VIRUS_SCAN_ENABLED

        self.parse_send_mail_config()

    def parse_scan_config(self):
        self.scan_cmd = VIRUS_SCAN_SCAN_COMMAND
        if not self.scan_cmd:
            logger.info(f'scan_command option is not found in seafile.conf, disable virus scan.')
            return False

        vcode = VIRUS_SCAN_VIRUS_CODE
        if not vcode:
            logger.info('virus_code is not set, disable virus scan.')
            return False

        nvcode = VIRUS_SCAN_NONVIRUS_CODE
        if not nvcode:
            logger.info('nonvirus_code is not set, disable virus scan.')
            return False

        vcodes = vcode.split(',')
        self.vir_codes = [code.strip() for code in vcodes if code]
        if len(self.vir_codes) == 0:
            logger.info('invalid virus_code format, disable virus scan.')
            return False

        nvcodes = nvcode.split(',')
        self.nonvir_codes = [code.strip() for code in nvcodes if code]
        if len(self.nonvir_codes) == 0:
            logger.info('invalid nonvirus_code format, disable virus scan.')
            return False

        self.scan_interval = VIRUS_SCAN_SCAN_INTERVAL

        self.scan_size_limit = VIRUS_SCAN_SCAN_SIZE_LIMIT

        if VIRUS_SCAN_SCAN_SKIP_EXT:
            exts = VIRUS_SCAN_SCAN_SKIP_EXT.split(',')
            # .jpg, .mp3, .mp4 format
            exts = [ext.strip() for ext in exts if ext]
            self.scan_skip_ext = [ext.lower() for ext in exts
                                  if len(ext) > 1 and ext[0] == '.']

        if VIRUS_SCAN_THREADS:
            self.threads = VIRUS_SCAN_THREADS

        return True

    def parse_send_mail_config(self):
        self.enable_send_mail = EMAIL_SENDER_ENABLED

    def is_enabled(self):
        return self.enable_scan
