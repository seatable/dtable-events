import requests
import logging
import time
from dtable_events.utils import gen_file_get_url, get_file_ext
from dtable_events.utils.constants import EXTRACT_TEXT_SUPPORTED_IMAGES
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
    def __init__(self, username, org_id, seatable_ai_server_url):
        self.username = username
        self.org_id = org_id
        self.seatable_ai_server_url = seatable_ai_server_url


    def summarize(self, content, summary_prompt):
        if not content or not content.strip():
            return ''
        
        data = {
            'content': f'content:{content}',
            'username': self.username,
            'org_id': self.org_id,
            'summary_prompt': summary_prompt,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/text-summarize/'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get('summary', '')
        else:
            logger.error(f"Failed to summarize text: {response.text}")
            raise DTableAIAPIError()

    def classify(self, content, classify_prompt=''):
        if not content or not content.strip():
            return []
        
        data = {
            'content': content,
            'username': self.username,
            'org_id': self.org_id,
            'classify_prompt': classify_prompt,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/classification/'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            classification = result.get('classification', [])
            return classification
        else:
            logger.error(f"Failed to classify text: {response.text}")
            raise DTableAIAPIError()

    def ocr(self, file_name, download_token):
        if get_file_ext(file_name) not in EXTRACT_TEXT_SUPPORTED_IMAGES:
            raise DTableAIAPIError("Unsupported image format")
        
        img_url = gen_file_get_url(download_token, file_name)

        data = {
            'username': self.username,
            'org_id': self.org_id,
            'img_url': img_url,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/ocr/'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get('ocr_result', '')
        else:
            logger.error(f"Failed to ocr image: {response.text}")
            raise DTableAIAPIError()

    def extract(self, content, extract_fields):
        """Extract specific information from content based on field descriptions"""
        if not content or not content.strip():
            return {}
        
        data = {
            'content': content,
            'username': self.username,
            'org_id': self.org_id,
            'extract_fields': extract_fields,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/extract/'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get('extracted_data', {})
        else:
            logger.error(f"Failed to extract information: {response.text}")
            raise DTableAIAPIError()

    def custom(self, content):
        """Execute custom AI processing with user-defined prompt"""
        if not content or not content.strip():
            return ''
        
        data = {
            'content': content,
            'username': self.username,
            'org_id': self.org_id,
        }
        
        url = f'{self.seatable_ai_server_url}/api/v1/ai/custom/'
        headers = gen_headers()
        response = requests.post(url, json=data, headers=headers, timeout=30)
        
        if response.status_code == 200:
            result = response.json()
            return result.get('result', '')
        else:
            logger.error(f"Failed to process custom AI request: {response.text}")
            raise DTableAIAPIError()
