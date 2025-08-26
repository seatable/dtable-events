# -*- coding: utf-8 -*-
import configparser
import logging
import os
import sys

logger = logging.getLogger(__name__)


# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist.')

sys.path.insert(0, dtable_web_dir)

try:
    import seahub.settings as seahub_settings
    DTABLE_WEB_SERVICE_URL = getattr(seahub_settings, 'DTABLE_WEB_SERVICE_URL', 'http://127.0.0.1')
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY', '')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    FILE_SERVER_ROOT = getattr(seahub_settings, 'FILE_SERVER_ROOT', 'http://127.0.0.1:8082')
    INNER_FILE_SERVER_ROOT = getattr(seahub_settings, 'INNER_FILE_SERVER_ROOT', 'http://127.0.0.1:8082')
    SEATABLE_FAAS_AUTH_TOKEN = getattr(seahub_settings, 'SEATABLE_FAAS_AUTH_TOKEN', '')
    ENABLE_PYTHON_SCRIPT = getattr(seahub_settings, 'ENABLE_PYTHON_SCRIPT', '')
    SEATABLE_FAAS_URL = getattr(seahub_settings, 'SEATABLE_FAAS_URL', '')
    SECRET_KEY = getattr(seahub_settings, 'SECRET_KEY', '')
    SESSION_COOKIE_NAME = getattr(seahub_settings, 'SESSION_COOKIE_NAME', 'sessionid')
    EXPORT2EXCEL_DEFAULT_STRING = getattr(seahub_settings, 'EXPORT2EXCEL_DEFAULT_STRING', 'illegal character in excel')
    TIME_ZONE = getattr(seahub_settings, 'TIME_ZONE', 'UTC')
    INNER_DTABLE_DB_URL = getattr(seahub_settings, 'INNER_DTABLE_DB_URL', 'http://127.0.0.1:7777')
    ENABLE_WEIXIN = getattr(seahub_settings, 'ENABLE_WEIXIN', False)
    ENABLE_WORK_WEIXIN = getattr(seahub_settings, 'ENABLE_WORK_WEIXIN', False)
    ENABLE_DINGTALK = getattr(seahub_settings, 'ENABLE_DINGTALK', False)
    USE_INNER_DTABLE_SERVER = getattr(seahub_settings, 'USE_INNER_DTABLE_SERVER', True)
    INNER_DTABLE_SERVER_URL = getattr(seahub_settings, 'INNER_DTABLE_SERVER_URL', 'http://127.0.0.1:5000/')
    ARCHIVE_VIEW_EXPORT_ROW_LIMIT = getattr(seahub_settings, 'ARCHIVE_VIEW_EXPORT_ROW_LIMIT', 250000)
    APP_TABLE_EXPORT_EXCEL_ROW_LIMIT = getattr(seahub_settings, 'APP_TABLE_EXPORT_EXCEL_ROW_LIMIT', 10000)
    BIG_DATA_ROW_IMPORT_LIMIT = getattr(seahub_settings, 'BIG_DATA_ROW_IMPORT_LIMIT', 500000)
    BIG_DATA_ROW_UPDATE_LIMIT = getattr(seahub_settings, 'BIG_DATA_ROW_UPDATE_LIMIT', 500000)
    TRASH_CLEAN_AFTER_DAYS = getattr(seahub_settings, 'TRASH_CLEAN_AFTER_DAYS', 30)
    LICENSE_PATH = getattr(seahub_settings, 'LICENSE_PATH', '/shared/seatable-license.txt')
    IS_PRO_VERSION = getattr(seahub_settings, 'IS_PRO_VERSION', False)
    ENABLE_OPERATION_LOG_DB = getattr(seahub_settings, 'ENABLE_OPERATION_LOG_DB', False)
    AI_PRICES = getattr(seahub_settings, 'AI_PRICES', {})
    BAIDU_OCR_TOKENS = getattr(seahub_settings, 'BAIDU_OCR_TOKENS', {})
    UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS = getattr(seahub_settings, 'UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS', 7)
    UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES = getattr(seahub_settings, 'UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES', 'auto backup')
    DTABLE_STORAGE_SERVER_URL = getattr(seahub_settings, 'DTABLE_STORAGE_SERVER_URL', 'http://127.0.0.1:6666')
    SEATABLE_AI_SERVER_URL = getattr(seahub_settings, 'SEATABLE_AI_SERVER_URL', 'http://127.0.0.1:8888')
    ENABLE_SEATABLE_AI = getattr(seahub_settings, 'ENABLE_SEATABLE_AI', False)
except Exception as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

def get_config(config_file):
    config = configparser.ConfigParser()
    try:
        config.read(config_file)
    except Exception as e:
        logger.critical("Failed to read config file %s: %s" % (config_file, e))
        raise RuntimeError("Failed to read config file %s: %s" % (config_file, e))

    return config


def is_syslog_enabled(config):
    if config.has_option('Syslog', 'enabled'):
        try:
            return config.getboolean('Syslog', 'enabled')
        except ValueError:
            return False

    return False


class TaskMode(object):
    enable_foreground_tasks = False
    enable_background_tasks = False


def get_task_mode(task_mode_str):
    task_mode = TaskMode()
    if task_mode_str == 'foreground':
        task_mode.enable_foreground_tasks = True
    elif task_mode_str == 'background':
        task_mode.enable_background_tasks = True
    else:
        task_mode.enable_foreground_tasks = True
        task_mode.enable_background_tasks = True

    return task_mode
