# -*- coding: utf-8 -*-
import os
import sys
import logging
from logging import handlers


def _get_log_level(level):
    if level == 'debug':
        return logging.DEBUG
    elif level == 'info':
        return logging.INFO
    else:
        return logging.WARNING


class LogConfigurator(object):
    def __init__(self, level, logfile=None):
        self._level = _get_log_level(level)
        self._logfile = logfile

        if logfile is None:
            self._basic_config()
        else:
            self._rotating_config()

    def _rotating_config(self):
        logging.root.setLevel(self._level)

        if os.environ.get('SEATABLE_LOG_TO_STDOUT', 'false') == 'true':
            # logs to stdout
            stdout_formatter = logging.Formatter('[dtable-events] [%(asctime)s] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
            stdout_handler = logging.StreamHandler()
            stdout_handler.setFormatter(stdout_formatter)
            logging.root.addHandler(stdout_handler)
        else:
            # logs to file
            file_formatter = logging.Formatter('[%(asctime)s] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
            file_handler = handlers.TimedRotatingFileHandler(self._logfile, when='W0', interval=1, backupCount=7)
            file_handler.setLevel(self._level)
            file_handler.setFormatter(file_formatter)
            logging.root.addHandler(file_handler)

        

    def _basic_config(self):
        # Log to stdout. Mainly for development.
        kw = {
            'format': '[dtable-events] [%(asctime)s] %(filename)s[line:%(lineno)d] [%(levelname)s] %(message)s',
            'datefmt': '%m/%d/%Y %H:%M:%S',
            'level': self._level,
            'stream': sys.stdout
        }

        logging.basicConfig(**kw)


def setup_logger(logname, fmt=None, level=None, propagate=None):
    """
    setup logger for dtable io
    """
    logger = logging.getLogger(logname)
    if propagate is not None:
        logger.propagate = propagate

    if os.environ.get('SEATABLE_LOG_TO_STDOUT', 'false') == 'true':
        # logs to stdout
        logger_component_name = logname.split('.')[0]
        stdout_handler = logging.StreamHandler()
        if level:
            stdout_handler.setLevel(level)
        if not fmt:
            fmt = f'[{logger_component_name}]' + '[%(asctime)s] [%(levelname)s] %(filename)s[line:%(lineno)d] %(message)s'
        stdout_formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        stdout_handler.setFormatter(stdout_formatter)
        stdout_handler.addFilter(logging.Filter(logname))
        logger.addHandler(stdout_handler)
    else:
        # logs to file
        logdir = os.path.join(os.environ.get('LOG_DIR', ''))
        log_file = os.path.join(logdir, logname)
        handler = handlers.TimedRotatingFileHandler(log_file, when='MIDNIGHT', interval=1, backupCount=7)
        if level:
            handler.setLevel(level)
        if not fmt:
            fmt = '[%(asctime)s] [%(levelname)s] %(filename)s[line:%(lineno)d] %(message)s'
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")
        handler.setFormatter(formatter)
        handler.addFilter(logging.Filter(logname))
        logger.addHandler(handler)

    return logger
