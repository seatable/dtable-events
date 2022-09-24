import json
import logging

import requests

from dtable_events.app.config import SEATABLE_FAAS_AUTH_TOKEN
from dtable_events.dtable_io.utils import get_dtable_server_token
from dtable_events.utils import uuid_str_to_36_chars


logger = logging.getLogger(__name__)


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class DTableWebAPI:

    def __init__(self, dtable_web_service_url, timeout=180):
        self.dtable_web_service_url = dtable_web_service_url.strip('/')
        self.timeout = timeout

    def get_related_users(self, dtable_uuid, username='dtable-events'):
        logger.debug('get related users dtable_uuid: %s, username: %s', dtable_uuid, username)
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = '%(server_url)s/api/v2.1/dtables/%(dtable_uuid)s/related-users/' % {
            'server_url': self.dtable_web_service_url,
            'dtable_uuid': dtable_uuid
        }
        access_token = get_dtable_server_token(username, dtable_uuid)
        headers = {'Authorization': 'Token ' + access_token}
        response = requests.get(url, headers=headers, timeout=self.timeout)
        return parse_response(response)['user_list']

    def can_user_run_python(self, user):
        logger.debug('can user run python user: %s', user)
        url = '%(server_url)s/api/v2.1/script-permissions/' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        json_data = {'users': [user]}
        # response dict like
        # {
        #   'user_script_permissions': {username1: {'can_run_python_script': True/False}}
        #   'can_schedule_run_script': {org1: {'can_run_python_script': True/False}}
        # }
        try:
            resp = requests.get(url, headers=headers, json=json_data, timeout=self.timeout)
            if resp.status_code != 200:
                logger.error('check run script permission error response: %s', resp.status_code)
                return False
            permission_dict = resp.json()
        except Exception as e:
            logger.error('check run script permission error: %s', e)
            return False
        return permission_dict['user_script_permissions'][user]['can_run_python_script']

    def can_org_run_python(self, org_id):
        logger.debug('can org run python org_id: %s', org_id)
        url = '%(server_url)s/api/v2.1/script-permissions/' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        json_data = {'org_ids': [org_id]}
        try:
            resp = requests.get(url, headers=headers, json=json_data, timeout=self.timeout)
            if resp.status_code != 200:
                logger.error('check run script permission error response: %s', resp.status_code)
                return False
            permission_dict = resp.json()
        except Exception as e:
            logger.error('check run script permission error: %s', e)
            return False
        return permission_dict['org_script_permissions'][str(org_id)]['can_run_python_script']

    def get_user_scripts_running_limit(self, user):
        logger.debug('get user scripts running limit user: %s', user)
        url = '%(server_url)s/api/v2.1/scripts-running-limit/' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        params = {'username': user}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=self.timeout)
            if resp.status_code != 200:
                logger.error('get scripts running limit error response: %s', resp.status_code)
                return 0
            scripts_running_limit = resp.json()['scripts_running_limit']
        except Exception as e:
            logger.error('get script running limit error: %s', e)
            return 0
        return scripts_running_limit

    def get_org_scripts_running_limit(self, org_id):
        logger.debug('get org scripts running limit user: %s', org_id)
        url = '%(server_url)s/api/v2.1/scripts-running-limit/' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        params = {'org_id': org_id}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=self.timeout)
            if resp.status_code != 200:
                logger.error('get scripts running limit error response: %s', resp.status_code)
                return 0
            scripts_running_limit = resp.json()['scripts_running_limit']
        except Exception as e:
            logger.error('get script running limit error: %s', e)
            return 0
        return scripts_running_limit
