import json
import logging
import requests
from dtable_events.dtable_io.utils import get_app_access_token

logger = logging.getLogger(__name__)


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


    def __init__(self, username, app_token, dtable_web_service_url):
        self.username = username
        self.app_token = app_token
        self.headers = None
        self.server_url = dtable_web_service_url.rstrip('/')
        self._init()

    def _init(self):
        access_token = get_app_access_token(self.username, self.app_token)
        self.headers = {'Authorization': 'Token ' + access_token}

    def batch_send_notification(self, user_msg_list):
        url = self.server_url + '/api/v2.1/universal-apps/' + self.app_token + '/notifications/?from=dtable_events'
        body = {
            'user_messages': user_msg_list,
        }
        response = requests.post(url, json=body, headers=self.headers)
        return parse_response(response)