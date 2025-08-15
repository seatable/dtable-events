import requests
import logging
import time
import jwt

from dtable_events.app.config import DTABLE_PRIVATE_KEY

logger = logging.getLogger(__name__)

class DTableAIAPIError(Exception):
    pass

def gen_headers():
    payload = {'exp': int(time.time()) + 300, }
    token = jwt.encode(payload, DTABLE_PRIVATE_KEY, algorithm='HS256')
    return {"Authorization": "Token %s" % token}


class DTableAIAPI:
    def __init__(self, username, seatable_ai_server_url):
        self.username = username
        self.seatable_ai_server_url = seatable_ai_server_url


    def summarize(self, content, requirement):
        if not content or not content.strip():
            return ''
        
        data = {
            'content': f'内容：{content}',
            'username': self.username,
            'requirement': requirement,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/text-summarize'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get('summary', '')
        else:
            logger.error(f"Failed to summarize text: {response.text}")
            raise DTableAIAPIError()
