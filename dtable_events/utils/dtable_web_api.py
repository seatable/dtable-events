import json
import logging

import requests

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

    def __init__(self, dtable_web_service_url):
        self.dtable_web_service_url = dtable_web_service_url.strip('/')

    def get_related_users(self, dtable_uuid, username='dtable-events'):
        logger.debug('get related users dtable_uuid: %s, username: %s', dtable_uuid, username)
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = '%(server_url)s/api/v2.1/dtables/%(dtable_uuid)s/related-users/' % {
            'server_url': self.dtable_web_service_url,
            'dtable_uuid': dtable_uuid
        }
        access_token = get_dtable_server_token(username, dtable_uuid)
        headers = {'Authorization': 'Token ' + access_token}
        response = requests.get(url, headers=headers)
        return parse_response(response)['user_list']
