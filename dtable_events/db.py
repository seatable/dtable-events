# -*- coding: utf-8 -*-
import configparser
import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


def is_enable_operation_log_db(config):
    db_sec = 'DATABASE'
    return config.getboolean(db_sec, 'enabled', fallback=False)


def create_engine_from_conf(config, db='dtable_db'):

    db_keys_map = {
        'dtable_db': {
            'db_section': 'DATABASE',
            'user': 'username',
            'host': 'host',
            'db_name': 'db_name',
            'password': 'password',
            'port': 'port'
        },
        'operation_log_db': {
            'db_section': 'DATABASE',
            'user': 'operation_log_db_username',
            'host': 'operation_log_db_host',
            'db_name': 'operation_log_db_name',
            'password': 'operation_log_db_password',
            'port': 'operation_log_db_port'
        }
    }

    db_section = db_keys_map[db]['db_section']
    user = db_keys_map[db]['user']
    host = db_keys_map[db]['host']
    db_name = db_keys_map[db]['db_name']
    password = db_keys_map[db]['password']
    port = db_keys_map[db]['port']

    backend = config.get(db_section, 'type')

    if backend == 'mysql':
        if config.has_option(db_section, host):
            host = config.get(db_section, host).lower()
        else:
            host = 'localhost'

        if config.has_option(db_section, port):
            port = config.getint(db_section, port)
        else:
            port = 3306

        username = config.get(db_section, user)
        password = config.get(db_section, password)
        db_name = config.get(db_section, db_name)

        db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % \
                 (username, quote_plus(password), host, port, db_name)
        logger.debug('[dtable_events] database: mysql, name: %s', db_name)
    else:
        logger.error("Unknown database backend: %s" % backend)
        raise RuntimeError("Unknown database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed
    # by mysql daemon if idle for too long.
    """MySQL has gone away
    https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
    https://docs.sqlalchemy.org/en/20/core/pooling.html#pool-disconnects
    """
    kwargs = dict(pool_recycle=300, pool_pre_ping=True, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    return engine


def init_db_session_class(config, db='dtable_db'):
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config, db=db)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Init db session class error: %s" % e)
        raise RuntimeError("Init db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def create_db_tables(config):
    # create events tables if not exists.
    try:
        engine = create_engine_from_conf(config)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Create tables error: %s" % e)
        raise RuntimeError("Create tables error: %s" % e)

    Base.metadata.create_all(engine)
