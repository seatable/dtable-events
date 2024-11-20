# coding: utf-8
import os
import logging

from dtable_events.utils import get_opt_from_conf_or_env, parse_bool
from dtable_events.db import init_db_session_class, init_seafile_db_session_class

logger = logging.getLogger('virus_scan')
logger.setLevel(logging.INFO)


class Settings(object):
    def __init__(self, config, seafile_config):
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

        self.parse_config(config, seafile_config)

    def parse_config(self, config, seafile_config):
        if not self.parse_scan_config(config):
            return

        try:
            self.session_cls = init_db_session_class(config)
            self.seaf_session_cls = init_seafile_db_session_class(seafile_config)
        except Exception as e:
            logger.warning('Failed to init db session class: %s', e)
            return

        section_name = 'VIRUS SCAN'
        self.enable_scan = config.getboolean(section_name, 'enabled', fallback=False)

        self.parse_send_mail_config(config)

    def parse_scan_config(self, config):
        section_name = 'VIRUS SCAN'
        if config.has_option(section_name, 'scan_command'):
            self.scan_cmd = config.get(section_name, 'scan_command')
        if not self.scan_cmd:
            logger.info(f'[{section_name}] scan_command option is not found in seafile.conf, disable virus scan.')
            return False

        vcode = None
        if config.has_option(section_name, 'virus_code'):
            vcode = config.get(section_name, 'virus_code')
        if not vcode:
            logger.info('virus_code is not set, disable virus scan.')
            return False

        nvcode = None
        if config.has_option(section_name, 'nonvirus_code'):
            nvcode = config.get(section_name, 'nonvirus_code')
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

        if config.has_option(section_name, 'scan_interval'):
            try:
                self.scan_interval = config.getint(section_name, 'scan_interval')
            except ValueError:
                pass

        if config.has_option(section_name, 'scan_size_limit'):
            try:
                # in M unit
                self.scan_size_limit = config.getint(section_name, 'scan_size_limit')
            except ValueError:
                pass

        if config.has_option(section_name, 'scan_skip_ext'):
            exts = config.get(section_name, 'scan_skip_ext').split(',')
            # .jpg, .mp3, .mp4 format
            exts = [ext.strip() for ext in exts if ext]
            self.scan_skip_ext = [ext.lower() for ext in exts
                                  if len(ext) > 1 and ext[0] == '.']

        if config.has_option(section_name, 'threads'):
            try:
                self.threads = config.getint(section_name, 'threads')
            except ValueError:
                pass

        return True

    def parse_send_mail_config(self, config):
        section_name = 'EMAIL SENDER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            return

        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        if not enabled:
            return

        self.enable_send_mail = True

    def is_enabled(self):
        return self.enable_scan
