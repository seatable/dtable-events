# -*- coding: utf-8 -*-
import configparser
import logging
import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import sessionmaker

from dtable_events.app.config import SEATABLE_MYSQL_DB_HOST, SEATABLE_MYSQL_DB_PORT, SEATABLE_MYSQL_DB_USER, \
    SEATABLE_MYSQL_DB_PASSWORD, SEATABLE_MYSQL_DB_DTABLE_DB_NAME, SEATABLE_MYSQL_DB_CCNET_DB_NAME, \
    SEATABLE_MYSQL_DB_SEAFILE_DB_NAME

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


SeafBase = automap_base()


def create_engine_from_conf(db):
    db_name = ''
    if db == 'dtable':
        db_name = SEATABLE_MYSQL_DB_DTABLE_DB_NAME
    elif db == 'seafile':
        db_name = SEATABLE_MYSQL_DB_SEAFILE_DB_NAME
    elif db == 'ccnet':
        db_name = SEATABLE_MYSQL_DB_CCNET_DB_NAME

    db_host = SEATABLE_MYSQL_DB_HOST
    db_port = SEATABLE_MYSQL_DB_PORT
    db_user = SEATABLE_MYSQL_DB_USER
    db_pwd = SEATABLE_MYSQL_DB_PASSWORD

    if not (db_name and db_host and db_port and db_user):
        raise RuntimeError('Database configured error')

    db_url = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % \
                 (db_user, quote_plus(db_pwd), db_host, db_port, db_name)
    logger.debug('[dtable_events] database: mysql, name: %s', db_name)

    # Add pool recycle, or mysql connection will be closed
    # by mysql daemon if idle for too long.
    """MySQL has gone away
    https://docs.sqlalchemy.org/en/20/faq/connections.html#mysql-server-has-gone-away
    https://docs.sqlalchemy.org/en/20/core/pooling.html#pool-disconnects
    """
    kwargs = dict(pool_recycle=300, pool_pre_ping=True, echo=False, echo_pool=False)

    engine = create_engine(db_url, **kwargs)

    return engine


def init_db_session_class():
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf('dtable')
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Init db session class error: %s" % e)
        raise RuntimeError("Init db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def init_seafile_db_session_class():
    """Configure session class for mysql according to the config file."""
    try:
        engine = create_engine_from_conf('seafile')
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Init operation-log db session class error: %s" % e)
        raise RuntimeError("Init operation-log db session class error: %s" % e)

    session = sessionmaker(bind=engine)
    return session


def create_db_tables():
    # create events tables if not exists.
    try:
        engine = create_engine_from_conf('dtable')
    except (configparser.NoOptionError, configparser.NoSectionError) as e:
        logger.error("Create tables error: %s" % e)
        raise RuntimeError("Create tables error: %s" % e)

    Base.metadata.create_all(engine)
    engine.dispose()


def prepare_seafile_tables():
    # reflect the seafile_db tables
    try:
        engine = create_engine_from_conf('seafile')
    except Exception as e:
        logger.error(e)
        raise RuntimeError("create db engine error: %s" % e)

    SeafBase.prepare(autoload_with=engine)
    engine.dispose()
