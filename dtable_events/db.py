# -*- coding: utf-8 -*-
import configparser
import logging
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


SeafBase = automap_base()


def create_engine_from_conf(config):
    backend = config.get('DATABASE', 'type') if config.has_section('DATABASE') and config.has_option('DATABASE', 'type') else 'mysql'

    if backend == 'mysql':
        if not (host := os.getenv('SEATABLE_MYSQL_DB_HOST')):
            if config.has_option('DATABASE', 'host'):
                host = config.get('DATABASE', 'host').lower()
            else:
                host = 'localhost'

        if not (port := os.getenv('SEATABLE_MYSQL_DB_PORT')):
            if config.has_option('DATABASE', 'port'):
                port = config.getint('DATABASE', 'port')
            else:
                port = 3306

        try:
            port = int(port)
        except:
            raise ValueError(f'Invalid database port: {port}')

        if not (username := os.getenv('SEATABLE_MYSQL_DB_USER')):
            username = config.get('DATABASE', 'username')

        if not (password := os.getenv('SEATABLE_MYSQL_DB_PASSWORD')):
            password = config.get('DATABASE', 'password')

        if not (db_name := os.getenv('SEATABLE_MYSQL_DB_DTABLE_DB_NAME')):
            db_name = config.get('DATABASE', 'db_name')

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


def create_seafile_engine_from_conf(config):
    backend = config.get('DATABASE', 'type') if config.has_section('DATABASE') and config.has_option('DATABASE', 'type') else 'mysql'

    if backend == 'mysql':
        if not (host := os.getenv('SEATABLE_MYSQL_DB_HOST')):
            if config.has_option('database', 'host'):
                host = config.get('database', 'host').lower()
            else:
                host = 'localhost'

        if not (port := os.getenv('SEATABLE_MYSQL_DB_PORT')):
            if config.has_option('database', 'port'):
                port = config.getint('database', 'port')
            else:
                port = 3306

        try:
            port = int(port)
        except:
            raise ValueError(f'Invalid database port: {port}')

        if not (username := os.getenv('SEATABLE_MYSQL_DB_USER')):
            username = config.get('database', 'user')

        if not (password := os.getenv('SEATABLE_MYSQL_DB_PASSWORD')):
            password = config.get('database', 'password')

        if not (db_name := os.getenv('SEATABLE_MYSQL_DB_SEAFILE_DB_NAME')):
            db_name = config.get('database', 'db_name')

        db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % \
                 (username, quote_plus(password), host, port, db_name)
        logger.debug('[dtable_events] seafile database: mysql, name: %s', db_name)
    else:
        logger.error("Unknown seafile database backend: %s" % backend)
        raise RuntimeError("Unknown seafile database backend: %s" % backend)

    # Add pool recycle, or mysql connection will be closed
    # by mysql daemon if idle for too long.
    """MySQL has gone away
    https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
    https://docs.sqlalchemy.org/en/20/core/pooling.html#pool-disconnects
    """
    kwargs = dict(pool_recycle=300, pool_pre_ping=True, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    return engine


def init_db_session_class(config):
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf(config)
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Init db session class error: %s" % e)
        raise RuntimeError("Init db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def init_seafile_db_session_class(config):
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_seafile_engine_from_conf(config)
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
    engine.dispose()


def prepare_seafile_tables(seafile_config):
    # reflect the seafile_db tables
    try:
        engine = create_seafile_engine_from_conf(seafile_config)
    except Exception as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    SeafBase.prepare(autoload_with=engine)
    engine.dispose()
