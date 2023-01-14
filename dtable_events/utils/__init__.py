# -*- coding: utf-8 -*-
import os
import sys
import logging
import configparser
import subprocess
import uuid

import pytz
import re

logger = logging.getLogger(__name__)
pyexec = None


EMAIL_RE = re.compile(
        r"(^[-!#$%&*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&*+/=?^_`{}|~0-9A-Z]+)*"  # dot-atom
        # quoted-string, see also http://tools.ietf.org/html/rfc2822#section-3.2.5
        r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-\011\013\014\016-\177])*"'
        r')@((?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)$)'  # domain
        r'|\[(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\]$',
        re.IGNORECASE)

def find_in_path(prog):
    if 'win32' in sys.platform:
        sep = ';'
    else:
        sep = ':'

    dirs = os.environ['PATH'].split(sep)
    for d in dirs:
        d = d.strip()
        if d == '':
            continue
        path = os.path.join(d, prog)
        if os.path.exists(path):
            return path

    return None


def parse_bool(v):
    if isinstance(v, bool):
        return v

    v = str(v).lower()

    if v == '1' or v == 'true':
        return True
    else:
        return False


def parse_interval(interval, default):
    if isinstance(interval, (int, int)):
        return interval

    interval = interval.lower()

    unit = 1
    if interval.endswith('s'):
        pass
    elif interval.endswith('m'):
        unit *= 60
    elif interval.endswith('h'):
        unit *= 60 * 60
    elif interval.endswith('d'):
        unit *= 60 * 60 * 24
    else:
        pass

    val = int(interval.rstrip('smhd')) * unit
    if val < 10:
        logger.warning('insane interval %s', val)
        return default
    else:
        return val


def get_opt_from_conf_or_env(config, section, key, env_key=None, default=None):
    # Get option value from events.conf.
    # If not specified in events.conf, check the environment variable.
    try:
        return config.get(section, key)
    except configparser.NoOptionError:
        if env_key is None:
            return default
        else:
            return os.environ.get(env_key.upper(), default)


def _get_python_executable():
    if sys.executable and os.path.isabs(sys.executable) and os.path.exists(sys.executable):
        return sys.executable

    try_list = [
        'python3.8',
        'python38',
        'python3.7',
        'python37',
        'python3.6',
        'python36',
    ]

    for prog in try_list:
        path = find_in_path(prog)
        if path is not None:
            return path

    path = os.environ.get('PYTHON', 'python')

    return path


def get_python_executable():
    # Find a suitable python executable
    global pyexec
    if pyexec is not None:
        return pyexec

    pyexec = _get_python_executable()
    return pyexec


def run(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    def quote(args):
        return ' '.join(['"%s"' % arg for arg in args])

    cmdline = quote(argv)
    # if cwd:
    #     logger.debug('Running command: %s, cwd = %s', cmdline, cwd)
    # else:
    #     logger.debug('Running command: %s', cmdline)

    with open(os.devnull, 'w') as devnull:
        kwargs = dict(cwd=cwd, env=env, shell=True)

        if suppress_stdout:
            kwargs['stdout'] = devnull
        if suppress_stderr:
            kwargs['stderr'] = devnull

        if output:
            kwargs['stdout'] = output
            kwargs['stderr'] = output

        return subprocess.Popen(cmdline, **kwargs)


def run_and_wait(argv, cwd=None, env=None, suppress_stdout=False, suppress_stderr=False, output=None):
    proc = run(argv, cwd, env, suppress_stdout, suppress_stderr, output)
    return proc.wait()


def utc_to_tz(dt, tz_str):
    # change from UTC timezone to another timezone
    tz = pytz.timezone(tz_str)
    utc = dt.replace(tzinfo=pytz.utc)
    # local = timezone.make_naive(utc, tz)
    # return local
    value = utc.astimezone(tz)
    if hasattr(tz, 'normalize'):
        # This method is available for pytz time zones.
        value = tz.normalize(value)
    return value.replace(tzinfo=None)

def uuid_str_to_36_chars(dtable_uuid):
    if len(dtable_uuid) == 32:
        return str(uuid.UUID(dtable_uuid))
    else:
        return dtable_uuid

def uuid_str_to_32_chars(dtable_uuid):
    if len(dtable_uuid) == 36:
        return uuid.UUID(dtable_uuid).hex
    else:
        return dtable_uuid

def is_valid_email(email):
    if email and (isinstance(email, str) or isinstance(email, bytes)):
        return EMAIL_RE.match(email) is not None
    return False

def get_inner_dtable_server_url():
    """ only for api
    """
    from dtable_events.app.config import ENABLE_DTABLE_SERVER_CLUSTER, DTABLE_PROXY_SERVER_URL, USE_INNER_DTABLE_SERVER, \
        INNER_DTABLE_SERVER_URL, DTABLE_SERVER_URL

    if ENABLE_DTABLE_SERVER_CLUSTER:
        return DTABLE_PROXY_SERVER_URL
    elif USE_INNER_DTABLE_SERVER:
        return INNER_DTABLE_SERVER_URL
    else:
        return DTABLE_SERVER_URL


def get_location_tree_json():
    import json
    from dtable_events.app.config import dtable_web_dir
    json_path = os.path.join(dtable_web_dir, 'media/geo-data/cn-location.json')

    with open(json_path, 'r', encoding='utf8') as fp:
        json_data = json.load(fp)

    return json_data


def normalize_file_path(path):
    """Remove '/' at the end of file path if necessary.
    And make sure path starts with '/'
    """

    path = path.strip('/')
    if path == '':
        return ''
    else:
        return '/' + path


def gen_file_get_url(token, filename):
    from urllib.parse import quote
    from dtable_events.app.config import FILE_SERVER_ROOT
    file_server_root = FILE_SERVER_ROOT.rstrip('/') if FILE_SERVER_ROOT else ''
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    return '%s/files/%s/%s' % (file_server_root, token, quote(filename))
