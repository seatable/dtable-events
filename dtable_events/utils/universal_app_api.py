import json
import logging
import time

import jwt
import requests

from dtable_events.app.config import DTABLE_PRIVATE_KEY

logger = logging.getLogger(__name__)


def get_app_access_token(username, app_uuid):
    payload = {
        'exp': int(time.time()) + 300,
        'app_uuid': app_uuid,
        'username': username,
        'permission': 'rw',
    }
    access_token = jwt.encode(
        payload, DTABLE_PRIVATE_KEY, algorithm='HS256'
    )

    return access_token


class WrongFilterException(Exception):
    pass


class NotFoundException(Exception):
    pass


def parse_response(response):
    if response.status_code >= 400:
        if response.status_code == 404:
            raise NotFoundException()
        try:
            response_json = response.json()
        except:
            pass
        else:
            if response_json.get('error_type') == 'wrong_filter_in_filters':
                raise WrongFilterException()
        raise ConnectionError(response.status_code, response.text)
    else:
        try:
            data = json.loads(response.text)
            return data
        except:
            pass


class UniversalAppAPI(object):


    def __init__(self, username, app_uuid, dtable_web_service_url):
        self.username = username
        self.app_uuid = app_uuid
        self.headers = None
        self.server_url = dtable_web_service_url.rstrip('/')
        self._init()

    def _init(self):
        access_token = get_app_access_token(self.username, self.app_uuid)
        self.headers = {'Authorization': 'Token ' + access_token}

    def batch_send_notification(self, user_msg_list):
        url = self.server_url + '/api/v2.1/universal-apps/' + self.app_uuid + '/notifications/?from=dtable_events'
        body = {
            'user_messages': user_msg_list,
        }
        response = requests.post(url, json=body, headers=self.headers)
        return parse_response(response)