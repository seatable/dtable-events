# -*- coding: utf-8 -*-
import configparser
import logging
import os

from dtable_events.app.config_parser import ConfigParser

logger = logging.getLogger(__name__)

central_conf_dir = os.environ.get('SEAFILE_CENTRAL_CONF_DIR', '')
yaml_file_path = os.path.join(central_conf_dir, os.environ.get('SEATABLE_CONFIG_NAME', 'seatable_config.yaml'))
configs = ConfigParser(yaml_file_path, 'dtable-events')

def validate_llm_models(models):
    if not models or not isinstance(models, list):
        return []
    validated_models = []

    for model in models:
        if not isinstance(model, dict) or model.get('disable', False):
            continue
        if model.get('type') in ('proxy', 'other', 'hosted_vllm'):
            required_fields = ('model', 'url')
        else:
            required_fields = ('model', 'key')
        if not all(field in model for field in required_fields):
            continue
        model['label'] = model.get('label', model['model'])
        validated_models.append(model)

    return validated_models


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

try:
    DTABLE_WEB_SERVICE_URL = configs.get('DTABLE_WEB_SERVICE_URL', default='http://127.0.0.1')
    DTABLE_PRIVATE_KEY = configs.get('JWT_PRIVATE_KEY', default='')
    ENABLE_DTABLE_SERVER_CLUSTER = configs.get('ENABLE_DTABLE_SERVER_CLUSTER', default=False)
    FILE_SERVER_ROOT = configs.get('FILE_SERVER_ROOT', default='http://127.0.0.1:8082')
    INNER_FILE_SERVER_ROOT = configs.get('INNER_FILE_SERVER_ROOT', default='http://127.0.0.1:8082')
    SEATABLE_FAAS_AUTH_TOKEN = configs.get('SEATABLE_FAAS_AUTH_TOKEN', default='')
    ENABLE_PYTHON_SCRIPT = configs.get('ENABLE_PYTHON_SCRIPT', default=False)
    SEATABLE_FAAS_URL = configs.get('SEATABLE_FAAS_URL', default='')
    SECRET_KEY = configs.get('SECRET_KEY', default='')
    EXPORT2EXCEL_DEFAULT_STRING = configs.get('EXPORT2EXCEL_DEFAULT_STRING', default='illegal character in excel')
    TIME_ZONE = configs.get('TIME_ZONE', default='UTC')
    INNER_DTABLE_DB_URL = configs.get('INNER_DTABLE_DB_URL', default='http://127.0.0.1:7777')
    ENABLE_WEIXIN = configs.get('ENABLE_WEIXIN', default=False)
    ENABLE_WORK_WEIXIN = configs.get('ENABLE_WORK_WEIXIN', default=False)
    ENABLE_DINGTALK = configs.get('ENABLE_DINGTALK', default=False)
    INNER_DTABLE_SERVER_URL = configs.get('INNER_DTABLE_SERVER_URL', default=False)
    ARCHIVE_VIEW_EXPORT_ROW_LIMIT = configs.get('ARCHIVE_VIEW_EXPORT_ROW_LIMIT', default=250000)
    APP_TABLE_EXPORT_EXCEL_ROW_LIMIT = configs.get('APP_TABLE_EXPORT_EXCEL_ROW_LIMIT', default=10000)
    BIG_DATA_ROW_IMPORT_LIMIT = configs.get('BIG_DATA_ROW_IMPORT_LIMIT', default=500000)
    BIG_DATA_ROW_UPDATE_LIMIT = configs.get('BIG_DATA_ROW_UPDATE_LIMIT', default=500000)
    TRASH_CLEAN_AFTER_DAYS = configs.get('TRASH_CLEAN_AFTER_DAYS', default=30)
    LICENSE_PATH = configs.get('LICENSE_PATH', default='/shared/seatable-license.txt')
    IS_PRO_VERSION = os.environ.get('IS_PRO_VERSION', default=True)
    ENABLE_OPERATION_LOG_DB = configs.get('ENABLE_OPERATION_LOG_DB', default=False)
    LLM_MODELS = []
    LLM_MODELS = validate_llm_models(configs.get('LLM_MODELS', LLM_MODELS))
    AI_PRICES = get_llm_prices(LLM_MODELS)
    BAIDU_OCR_TOKENS = configs.get('BAIDU_OCR_TOKENS', default={})
    UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS = configs.get('UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_DAYS', default=7)
    UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES = configs.get('UNIVERSAL_APP_SNAPSHOT_AUTO_SAVE_NOTES', default='auto backup')
    DTABLE_STORAGE_SERVER_URL = configs.get('DTABLE_STORAGE_SERVER_URL', default='http://127.0.0.1:6666')
    INNER_SEATABLE_AI_SERVER_URL = configs.get('INNER_SEATABLE_AI_SERVER_URL', default='http://127.0.0.1:8888')
    ENABLE_SEATABLE_AI = configs.get('ENABLE_SEATABLE_AI', default=False)
    AUTO_RULES_AI_CONTENT_MAX_LENGTH = configs.get('AUTO_RULES_AI_CONTENT_MAX_LENGTH', default=10000)
    ORG_MEMBER_QUOTA_DEFAULT = configs.get('ORG_MEMBER_QUOTA_DEFAULT', 10)

    # env
    CCNET_DB_NAME = os.environ.get('SEATABLE_MYSQL_DB_CCNET_DB_NAME', 'ccnet_db')
    TIME_ZONE = os.environ.get('TIME_ZONE', 'UTC')
except Exception as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)

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
