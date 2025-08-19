import json
import logging
import time

import jwt
import requests

from dtable_events.app.config import SEATABLE_FAAS_URL, SEATABLE_FAAS_AUTH_TOKEN, DTABLE_PRIVATE_KEY
from dtable_events.utils import uuid_str_to_36_chars


logger = logging.getLogger(__name__)


def get_access_token(username, dtable_uuid, timeout=300, is_internal=False):
    payload = {
        'exp': int(time.time()) + timeout,
        'dtable_uuid': dtable_uuid,
        'username': username,
        'permission': 'rw',
    }
    if is_internal:
        payload['is_internal'] = True
    access_token = jwt.encode(
        payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
    )

    return access_token


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

    def __init__(self, dtable_web_service_url):
        self.dtable_web_service_url = dtable_web_service_url.strip('/')

    def get_related_users(self, dtable_uuid, username='dtable-events'):
        logger.debug('get related users dtable_uuid: %s, username: %s', dtable_uuid, username)
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = '%(server_url)s/api/v2.1/dtables/%(dtable_uuid)s/related-users/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url,
            'dtable_uuid': dtable_uuid
        }
        access_token = get_access_token(username, dtable_uuid)
        headers = {'Authorization': 'Token ' + access_token}
        response = requests.get(url, headers=headers)
        return parse_response(response)

    def can_user_run_python(self, user):
        logger.debug('can user run python user: %s', user)
        url = '%(server_url)s/api/v2.1/script-permissions/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        json_data = {'users': [user]}
        # response dict like
        # {
        #   'user_script_permissions': {username1: {'can_run_python_script': True/False}}
        #   'org_script_permissions': {org1: {'can_run_python_script': True/False}}
        # }
        try:
            resp = requests.get(url, headers=headers, json=json_data)
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
        url = '%(server_url)s/api/v2.1/script-permissions/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        json_data = {'org_ids': [org_id]}
        try:
            resp = requests.get(url, headers=headers, json=json_data)
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
        url = '%(server_url)s/api/v2.1/scripts-running-limit/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        params = {'username': user}
        try:
            resp = requests.get(url, headers=headers, params=params)
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
        url = '%(server_url)s/api/v2.1/scripts-running-limit/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url
        }
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        params = {'org_id': org_id}
        try:
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                logger.error('get scripts running limit error response: %s', resp.status_code)
                return 0
            scripts_running_limit = resp.json()['scripts_running_limit']
        except Exception as e:
            logger.error('get script running limit error: %s', e)
            return 0
        return scripts_running_limit

    def get_script_api_token(self, dtable_uuid, username=None, app_name=None):
        payload = {
            'dtable_uuid': uuid_str_to_36_chars(dtable_uuid),
            'exp': int(time.time()) + 60 * 60,
        }
        if username:
            payload['username'] = username
        if app_name:
            payload['app_name'] = app_name
        temp_api_token = jwt.encode(payload, SEATABLE_FAAS_AUTH_TOKEN, algorithm='HS256')
        return temp_api_token

    def run_script(self, dtable_uuid, script_name, context_data, owner, org_id, scripts_running_limit, operate_from, operator):
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        url = SEATABLE_FAAS_URL.strip('/') + '/run-script/'
        response = requests.post(url, json={
            'dtable_uuid': uuid_str_to_36_chars(dtable_uuid),
            'script_name': script_name,
            'context_data': context_data,
            'owner': owner,
            'org_id': org_id,
            'temp_api_token': self.get_script_api_token(dtable_uuid, app_name=script_name),
            'scripts_running_limit': scripts_running_limit,
            'operate_from': operate_from,
            'operator': operator
        }, headers=headers, timeout=10)
        return parse_response(response)

    def get_users_common_info(self, user_id_list):
        url = self.dtable_web_service_url + '/api/v2.1/users-common-info/'
        json_data = {
                'user_id_list': user_id_list,
            }
        payload = {
            'exp': int(time.time()) + 60
        }
        access_token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
        headers = {'Authorization': 'Token ' + access_token}
        res = requests.post(url, headers=headers, json=json_data)
        return parse_response(res)

    def internal_add_notification(self, to_users, msg_type, detail):
        logger.debug('internal add notification to users: %s detail: %s', to_users, detail)
        url = '%(server_url)s/api/v2.1/internal-notifications/?from=dtable_events' % {
            'server_url': self.dtable_web_service_url
        }
        payload = {
            'exp': int(time.time()) + 60
        }
        token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
        headers = {'Authorization': 'Token ' + token}
        resp = requests.post(url, json={
            'detail': detail,
            'to_users': to_users,
            'type': msg_type
        }, headers=headers)
        return parse_response(resp)

    def internal_submit_row_workflow(self, workflow_token, row_id, rule_id=None):
        logger.debug('internal submit row workflow token: %s row_id: %s rule_id: %s', workflow_token, row_id, rule_id)
        url = '%(server_url)s/api/v2.1/workflows/%(workflow_token)s/internal-task-submit/' % {
            'server_url': self.dtable_web_service_url,
            'workflow_token': workflow_token
        }
        data = {
            'row_id': row_id,
            'replace': 'true',
            'submit_from': 'Automation Rule'
        }
        if rule_id:
            data['automation_rule_id'] = rule_id
        payload = {
            'exp': int(time.time()) + 60,
            'token': workflow_token
        }
        header_token = 'Token ' + jwt.encode(payload, DTABLE_PRIVATE_KEY, 'HS256')
        resp = requests.post(url, data=data, headers={'Authorization': header_token}, timeout=30)
        return parse_response(resp)

    def internal_update_exceed_api_quota(self, month, org_ids, owner_ids):
        logger.debug('internal update exeed api quota for month %s, org_ids are %s', month, org_ids)
        url = '%(server_url)s/api/v2.1/internal/update-exceed-api-quota/' % {
            'server_url': self.dtable_web_service_url
        }
        data = {
            'org_ids': org_ids,
            'owner_ids': owner_ids,
            'month': month
        }
        payload = {
            'exp': int(time.time()) + 60
        }
        header_token = 'Token ' + jwt.encode(payload, DTABLE_PRIVATE_KEY, 'HS256')
        resp = requests.post(url, json=data, headers={'Authorization': header_token}, timeout=30)
        return parse_response(resp)

    def ai_permission_check(self, dtable_uuid):
        logger.debug('ai permission check for dtable_uuid: %s', dtable_uuid)
        url = '%(server_url)s/api/v2.1/ai/ai-permission/' % {
            'server_url': self.dtable_web_service_url
        }
        params = {'dtable_uuid': dtable_uuid}
        payload = {
            'exp': int(time.time()) + 60
        }
        header_token = 'Token ' + jwt.encode(payload, DTABLE_PRIVATE_KEY, 'HS256')
        resp = requests.get(url, params=params, headers={'Authorization': header_token}, timeout=30)
        return parse_response(resp)