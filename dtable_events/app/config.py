# -*- coding: utf-8 -*-
import configparser
import logging
import os

from dtable_events.app.config_parser import ConfigParser

dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '/opt/seatable/seatable-server-latest/dtable-web')

logger = logging.getLogger(__name__)

central_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', '')
yaml_file_path = os.path.join(central_conf_dir, os.environ.get('SEATABLE_CONFIG_NAME', 'seatable_config.yaml'))
configs = ConfigParser(yaml_file_path, 'dtable-events')

def get_llm_prices(models):
    if not models or not isinstance(models, list):
        return {}
    prices = {}
    for model in models:
        if not isinstance(model, dict):
            continue
        model_name = model.get('model')
        price = model.get('price')
        if not model_name or not isinstance(price, dict):
            continue
        if 'input_tokens' not in price:
            continue
        prices[model_name] = {
            'input_tokens': price.get('input_tokens', 0),
            'output_tokens': price.get('output_tokens', 0),
        }
    return prices

TIME_ZONE = configs.get('TIME_ZONE', default='UTC')

INNER_DTABLE_WEB_SERVICE_URL = configs.get('INNER_DTABLE_WEB_SERVICE_URL', default='http://127.0.0.1')

DTABLE_WEB_SERVICE_URL = ''
seatable_server_protocol = configs.get('SEATABLE_SERVER_PROTOCOL', 'http')
seatable_server_hostname = configs.get('SEATABLE_SERVER_HOSTNAME', '')
if seatable_server_protocol and seatable_server_hostname:
    DTABLE_WEB_SERVICE_URL = seatable_server_protocol + '://' + seatable_server_hostname.rstrip('/')

DTABLE_PRIVATE_KEY = configs.get('JWT_PRIVATE_KEY', default='')
SECRET_KEY = configs.get('SECRET_KEY', default='')

# mysql
SEATABLE_MYSQL_DB_HOST = configs.get('SEATABLE_MYSQL_DB_HOST', default='')
SEATABLE_MYSQL_DB_PORT = configs.get('SEATABLE_MYSQL_DB_PORT', default=3306)
SEATABLE_MYSQL_DB_USER = configs.get('SEATABLE_MYSQL_DB_USER', default='root')
SEATABLE_MYSQL_DB_PASSWORD = configs.get('SEATABLE_MYSQL_DB_PASSWORD', default='')
SEATABLE_MYSQL_DB_DTABLE_DB_NAME = configs.get('SEATABLE_MYSQL_DB_DTABLE_DB_NAME', default='dtable_db')
SEATABLE_MYSQL_DB_CCNET_DB_NAME = configs.get('SEATABLE_MYSQL_DB_CCNET_DB_NAME', default='ccnet_db')
SEATABLE_MYSQL_DB_SEAFILE_DB_NAME = configs.get('SEATABLE_MYSQL_DB_SEAFILE_DB_NAME', default='seafile_db')
ENABLE_OPERATION_LOG_DB = configs.get('ENABLE_OPERATION_LOG_DB', default=False)

# redis
REDIS_HOST = configs.get('REDIS_HOST', default='127.0.0.1')
REDIS_PORT = configs.get('REDIS_PORT', default=6379)
REDIS_PASSWORD = configs.get('REDIS_PASSWORD', default='')

# inner server url
INNER_DTABLE_SERVER_URL = configs.get('INNER_DTABLE_SERVER_URL', default='http://127.0.0.1:5000')
INNER_DTABLE_DB_URL = configs.get('INNER_DTABLE_DB_URL', default='http://127.0.0.1:7777')

# file server
FILE_SERVER_ROOT = DTABLE_WEB_SERVICE_URL.strip('/') + '/seafhttp'
INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:8082'

# python runner
ENABLE_PYTHON_SCRIPT = configs.get('ENABLE_PYTHON_SCRIPT', default=False)
SEATABLE_FAAS_URL = configs.get('PYTHON_SCHEDULER_URL', default='http://python-scheduler')
SEATABLE_FAAS_AUTH_TOKEN = configs.get('PYTHON_SCHEDULER_AUTH_TOKEN', default='')

# AI
ENABLE_SEATABLE_AI = configs.get('ENABLE_SEATABLE_AI', default=False)
INNER_SEATABLE_AI_SERVER_URL = configs.get('INNER_SEATABLE_AI_SERVER_URL', default='http://127.0.0.1:8888')
AUTO_RULES_AI_CONTENT_MAX_LENGTH = configs.get('AUTO_RULES_AI_CONTENT_MAX_LENGTH', default=10000)

# AI models and prices
LLM_MODELS = configs.get('LLM_MODELS', [])
AI_PRICES = get_llm_prices(LLM_MODELS)
BAIDU_OCR_TOKENS = configs.get('BAIDU_OCR_TOKENS', default={})

# IO server
ARCHIVE_VIEW_EXPORT_ROW_LIMIT = configs.get('ARCHIVE_VIEW_EXPORT_ROW_LIMIT', default=250000)
APP_TABLE_EXPORT_EXCEL_ROW_LIMIT = configs.get('APP_TABLE_EXPORT_EXCEL_ROW_LIMIT', default=10000)
BIG_DATA_ROW_IMPORT_LIMIT = configs.get('BIG_DATA_ROW_IMPORT_LIMIT', default=500000)
BIG_DATA_ROW_UPDATE_LIMIT = configs.get('BIG_DATA_ROW_UPDATE_LIMIT', default=500000)

# notices feature
ENABLE_WEIXIN = configs.get('ENABLE_WEIXIN', default=False)
ENABLE_WORK_WEIXIN = configs.get('ENABLE_WORK_WEIXIN', default=False)
ENABLE_DINGTALK = configs.get('ENABLE_DINGTALK', default=False)

# trash clean
TRASH_CLEAN_AFTER_DAYS = configs.get('TRASH_CLEAN_AFTER_DAYS', default=30)

# license
LICENSE_PATH = configs.get('LICENSE_PATH', default='/opt/seatable/seatable-license.txt')

IS_PRO_VERSION = os.environ.get('IS_PRO_VERSION', default=True)

# universal app snapshots
UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS = configs.get('UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS', default=7)

# storage server
DTABLE_STORAGE_SERVER_URL = configs.get('DTABLE_STORAGE_SERVER_URL', default='http://127.0.0.1:6666')

# org member quota
ORG_MEMBER_QUOTA_DEFAULT = configs.get('ORG_MEMBER_QUOTA_DEFAULT', 10)

# log
LOG_LEVEL = configs.get('LOG_LEVEL', default='info')
LOG_DIR = configs.get('LOG_DIR', '/opt/seatable/logs')
SEATABLE_LOG_TO_STDOUT = configs.get('SEATABLE_LOG_TO_STDOUT', default=False)

# IO server
IO_SERVER_HOST = configs.get('IO_SERVER_HOST', default='127.0.0.1')
IO_SERVER_PORT = configs.get('IO_SERVER_PORT', default=6000)
IO_SERVER_WORKERS = configs.get('IO_SERVER_WORKERS', default=3)
IO_SERVER_TASK_TIMEOUT = configs.get('IO_SERVER_TASK_TIMEOUT', default=3600)

# instant notices sender
INSTANT_SENDER_INTERVAL = configs.get('INSTANT_SENDER_INTERVAL', default=60)

# email notices sender
EMAIL_SENDER_ENABLED = configs.get('EMAIL_SENDER_ENABLED', default=True)
EMAIL_SENDER_INTERVAL = configs.get('EMAIL_SENDER_INTERVAL', default=60*60)

# updates sender
UPDATES_SENDER_ENABLED = configs.get('UPDATES_SENDER_ENABLED', default=True)

# notification rules scanner
NOTIFICATION_RULES_SCAN_ENABLED = configs.get('NOTIFICATION_RULES_SCAN_ENABLED', default=True)

# LDAP sync
LDAP_SYNC_ENABLED = configs.get('LDAP_SYNC_ENABLED', default=False)
LDAP_SYNC_INTERVAL = configs.get('LDAP_SYNC_INTERVAL', default=60*60)

# common dataset syncer
COMMON_DATASET_SYNCER_ENABLED = configs.get('COMMON_DATASET_SYNCER_ENABLED', default=True)

# clean db
CLEAN_DB_ENABLED = configs.get('CLEAN_DB_ENABLED', default=True)
CLEAN_DB_KEEP_DTABLE_SNAPSHOT_DAYS = configs.get('CLEAN_DB_KEEP_DTABLE_SNAPSHOT_DAYS', default=365)
CLEAN_DB_KEEP_ACTIVITIES_DAYS = configs.get('CLEAN_DB_KEEP_ACTIVITIES_DAYS', default=30)
CLEAN_DB_KEEP_OPERATION_LOG_DAYS = configs.get('CLEAN_DB_KEEP_OPERATION_LOG_DAYS', default=14)
CLEAN_DB_KEEP_DELETE_OPERATION_LOG_DAYS = configs.get('CLEAN_DB_KEEP_DELETE_OPERATION_LOG_DAYS', default=30)
CLEAN_DB_KEEP_DTABLE_DB_OP_LOG_DAYS = configs.get('CLEAN_DB_KEEP_DTABLE_DB_OP_LOG_DAYS', default=30)
CLEAN_DB_KEEP_NOTIFICATIONS_USERNOTIFICATION_DAYS = configs.get('CLEAN_DB_KEEP_NOTIFICATIONS_USERNOTIFICATION_DAYS', default=30)
CLEAN_DB_KEEP_DTABLE_NOTIFICATIONS_DAYS = configs.get('CLEAN_DB_KEEP_DTABLE_NOTIFICATIONS_DAYS', default=30)
CLEAN_DB_KEEP_SESSION_LOG_DAYS = configs.get('CLEAN_DB_KEEP_SESSION_LOG_DAYS', default=30)
CLEAN_DB_KEEP_AUTO_RULES_TASK_LOG_DAYS = configs.get('CLEAN_DB_KEEP_AUTO_RULES_TASK_LOG_DAYS', default=30)
CLEAN_DB_KEEP_USER_ACTIVITY_STATISTICS_DAYS = configs.get('CLEAN_DB_KEEP_USER_ACTIVITY_STATISTICS_DAYS', default=0)
CLEAN_DB_KEEP_DTABLE_APP_PAGES_OPERATION_LOG_DAYS = configs.get('CLEAN_DB_KEEP_DTABLE_APP_PAGES_OPERATION_LOG_DAYS', default=14)
CLEAN_DB_KEEP_EMAIL_SENDING_LOG_DAYS = configs.get('CLEAN_DB_KEEP_EMAIL_SENDING_LOG_DAYS', default=30)
CLEAN_DB_KEEP_SYSADMIN_EXTRA_USERLOGINLOG_DAYS = configs.get('CLEAN_DB_KEEP_SYSADMIN_EXTRA_USERLOGINLOG_DAYS', default=30)

# email syncer
EMAIL_SYNCER_ENABLED = configs.get('EMAIL_SYNCER_ENABLED', default=True)
EMAIL_SYNCER_MAX_WORKERS = configs.get('EMAIL_SYNCER_MAX_WORKERS', default=5)

# workflow scanner
WORKFLOW_SCANNER_ENABLED = configs.get('WORKFLOW_SCANNER_ENABLED', default=True)

# AI stats
AI_STATS_ENABLED = configs.get('AI_STATS_ENABLED', default=True)

# automation
AUTOMATION_WORKERS = configs.get('AUTOMATION_WORKERS', default=5)
AUTOMATION_RATE_LIMIT_WINDOW_SECS = configs.get('AUTOMATION_RATE_LIMIT_WINDOW_SECS', default=300)
AUTOMATION_RATE_LIMIT_PERCENT = configs.get('AUTOMATION_RATE_LIMIT_PERCENT', default=0.25)

# playwright
CONVERT_PDF_BROWSERS = configs.get('CONVERT_PDF_BROWSERS', default=2)
CONVERT_PDF_SESSIONS_PER_BROWSER = configs.get('CONVERT_PDF_SESSIONS_PER_BROWSER', default=3)

# virus code
VIRUS_SCAN_ENABLED = configs.get('VIRUS_SCAN_ENABLED', default=False)
VIRUS_SCAN_SCAN_COMMAND = configs.get('VIRUS_SCAN_SCAN_COMMAND')
VIRUS_SCAN_VIRUS_CODE = configs.get('VIRUS_SCAN_VIRUS_CODE')
VIRUS_SCAN_NONVIRUS_CODE = configs.get('VIRUS_SCAN_NONVIRUS_CODE')
VIRUS_SCAN_SCAN_INTERVAL = configs.get('VIRUS_SCAN_SCAN_INTERVAL', default=60)
VIRUS_SCAN_SCAN_SIZE_LIMIT = configs.get('VIRUS_SCAN_SCAN_SIZE_LIMIT', default=20)
VIRUS_SCAN_SCAN_SKIP_EXT = configs.get('VIRUS_SCAN_SCAN_SKIP_EXT')
VIRUS_SCAN_THREADS = configs.get('VIRUS_SCAN_THREADS', default=4)

def get_config(config_file):
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        logger.debug("Config file %s not found" % config_file)
        return config
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
