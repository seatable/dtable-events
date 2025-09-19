import logging
import time
import requests
from urllib import parse
from abc import ABC, abstractmethod

from dtable_events.app.log import setup_logger
from dtable_events.automations.models import get_third_party_account, update_third_party_account_detail
from dtable_events.db import init_db_session_class

dtable_calendar_logger = setup_logger('dtable_events_calendar', propagate=False)

class ThirdPartyAccountNotFound(Exception):
    pass

class ThirdPartyAccountInvalid(Exception):
    pass

class ThirdPartyAccountFetchTokenFailure(Exception):
    pass

def _check_and_raise_error(response):
    if response.status_code >= 400:
        raise ConnectionError(response.json())

class _ThirdpartyAPICalendarBase(ABC):
    
    def __init__(self, db_session, account_id, detail):
        self.account_id = account_id
        self.detail = detail
        self.db_session = db_session
        self.client_id = detail.get('client_id')
        self.client_secret = detail.get('client_secret')
        self.refresh_token = detail.get('refresh_token')
        self.access_token = detail.get('access_token')
        self.expires_at = detail.get('expires_at')
        self.token_url = detail.get('token_url', 'https://oauth2.googleapis.com/token')
        self.scopes = detail.get('scopes', ['https://www.googleapis.com/auth/calendar'])

        # Check required OAuth2 fields
        required_fields = [self.client_id, self.client_secret, self.refresh_token]
        if not all(required_fields):
            missing_fields = [name for name, value in zip(['client_id', 'client_secret', 'refresh_token'], required_fields) if not value]
            dtable_calendar_logger.error(f'Missing required fields for account {self.account_id}: {missing_fields}')
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id)
        
        self._request_access_token()
        
    def _request_access_token(self):
        if not self.access_token or not self.expires_at or self.expires_at - time.time() < 300:
            params = {
                'grant_type': 'refresh_token',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'refresh_token': self.refresh_token,
                'scope': ' '.join(self.scopes)
            }
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            response = requests.post(self.token_url, headers=headers, data=parse.urlencode(params))
            _check_and_raise_error(response)
            response = response.json()
            if not response.get('access_token'):
                raise ThirdPartyAccountFetchTokenFailure(f'Failure to fetch access token, account_id: {self.account_id}')
            expires_at = 0
            if response.get('ext_expires_at'):
                expires_at = response.get('ext_expires_at')
            elif response.get('expires_at'):
                expires_at = response.get('expires_at')
            elif response.get('ext_expires_in'):
                expires_at = response.get('ext_expires_in') + time.time()
            elif response.get('expires_in'):
                expires_at = response.get('expires_in') + time.time()
            self._check_and_update_refresh_token(response.get('access_token'), expires_at, response.get('refresh_token') or self.refresh_token)

    def _check_and_update_refresh_token(self, new_access_token, new_expires_at, new_refresh_token):
        self.access_token = new_access_token
        self.expires_at = new_expires_at
        self.refresh_token = new_refresh_token
        self.detail['access_token'] = new_access_token
        self.detail['expires_at'] = new_expires_at
        self.detail['refresh_token'] = new_refresh_token
        if self.db_session:
            update_third_party_account_detail(self.db_session, self.account_id, self.detail)

    def _make_api_request(self, method, url, data=None, params=None):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=30)
            elif method == 'POST':
                response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
            elif method == 'PUT':
                response = requests.put(url, headers=headers, json=data, timeout=30)
            elif method == 'DELETE':
                response = requests.delete(url, headers=headers, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # handle token expiration
            if response.status_code == 401:
                self._request_access_token()
                headers['Authorization'] = f'Bearer {self.access_token}'
                
                # retry request
                if method == 'GET':
                    response = requests.get(url, headers=headers, params=params, timeout=30)
                elif method == 'POST':
                    response = requests.post(url, headers=headers, json=data, params=params, timeout=30)
                elif method == 'PUT':
                    response = requests.put(url, headers=headers, json=data, timeout=30)
                elif method == 'DELETE':
                    response = requests.delete(url, headers=headers, timeout=30)
            
            return response
            
        except requests.RequestException as e:
            dtable_calendar_logger.error(f"Request failed: {str(e)}")
            raise Exception(f"Request failed: {str(e)}")

    @abstractmethod
    def create_event(self, event_data): ...

    @abstractmethod
    def update_event(self, event_data): ...

    @abstractmethod
    def delete_event(self, event_data): ...

    @abstractmethod
    def get_events(self, query_data): ...


class GoogleCalendarAPI(_ThirdpartyAPICalendarBase):
    
    def __init__(self, db_session, account_id, detail):
        super().__init__(db_session, account_id, detail)
        self.base_url = 'https://www.googleapis.com/calendar/v3'

    def create_event(self, event_data):
        calendar_id = event_data.get('calendar_id', 'primary')
        url = f"{self.base_url}/calendars/{parse.quote(calendar_id)}/events"
        params = {'conferenceDataVersion': 1}
        
        response = self._make_api_request('POST', url, data=event_data, params=params)
        
        try:
            _check_and_raise_error(response)
            return response.json()
        except Exception as e:
            dtable_calendar_logger.error(f'Failed to create Google Calendar event: {str(e)}')
            raise

    def update_event(self, event_data):
        calendar_id = event_data.get('calendar_id', 'primary')
        event_id = event_data.get('event_id')
        
        if not event_id:
            raise ValueError("Event ID is required for updating")
        
        url = f"{self.base_url}/calendars/{parse.quote(calendar_id)}/events/{event_id}"
        
        response = self._make_api_request('PUT', url, data=event_data)
        
        try:
            _check_and_raise_error(response)
            return response.json()
        except Exception as e:
            dtable_calendar_logger.error(f'Failed to update Google Calendar event: {str(e)}')
            raise

    def delete_event(self, event_data):
        calendar_id = event_data.get('calendar_id', 'primary')
        event_id = event_data.get('event_id')
        
        if not event_id:
            raise ValueError("Event ID is required for deleting")
        
        url = f"{self.base_url}/calendars/{parse.quote(calendar_id)}/events/{event_id}"
        
        response = self._make_api_request('DELETE', url)
        
        try:
            _check_and_raise_error(response)
            return {'success': True}
        except Exception as e:
            dtable_calendar_logger.error(f'Failed to delete Google Calendar event: {str(e)}')
            raise

    def get_events(self, query_data):
        calendar_id = query_data.get('calendar_id', 'primary')
        url = f"{self.base_url}/calendars/{parse.quote(calendar_id)}/events"
        
        params = {}
        if query_data.get('time_min'):
            params['timeMin'] = query_data['time_min']
        if query_data.get('time_max'):
            params['timeMax'] = query_data['time_max']
        if query_data.get('max_results'):
            params['maxResults'] = query_data['max_results']
        if query_data.get('page_token'):
            params['pageToken'] = query_data['page_token']
        if query_data.get('order_by'):
            params['orderBy'] = query_data['order_by']
        if query_data.get('single_events') is not None:
            params['singleEvents'] = query_data['single_events']
        
        response = self._make_api_request('GET', url, params=params)
        
        try:
            _check_and_raise_error(response)
            return response.json()
        except Exception as e:
            dtable_calendar_logger.error(f'Failed to get Google Calendar events: {str(e)}')
            raise

    def get_calendar_list(self):
        url = f"{self.base_url}/users/me/calendarList"
        
        response = self._make_api_request('GET', url)
        
        try:
            _check_and_raise_error(response)
            return response.json()
        except Exception as e:
            dtable_calendar_logger.error(f'Failed to get Google Calendar list: {str(e)}')
            raise


class CalendarManager:
    def __init__(self, account_id, config=None, db_session=None):
        """
        one of config or db_session is required
        """
        self.account_id = account_id
        self.config = config
        self.is_config_session = False if db_session else True
        self.db_session = db_session or init_db_session_class(self.config)()
        self._manager_init()

    def _manager_init(self):
        account_info = get_third_party_account(self.db_session, self.account_id)
        if not account_info:
            raise ThirdPartyAccountNotFound('Third party account %s does not exists.' % self.account_id)
        detail = account_info.get('detail')
        if not detail:
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id)

        self.calendar_provider = detail.get('calendar_provider', 'GoogleCalendar')

        if self.calendar_provider == 'GoogleCalendar':
            self.manager = GoogleCalendarAPI(self.db_session, self.account_id, detail)
        else:
            raise ThirdPartyAccountInvalid(f'Invalid third-party calendar account_id: {self.account_id} account type: {self.calendar_provider}')

    def create_event(self, event_data):
        try:
            return self.manager.create_event(event_data)
        except Exception as e:
            dtable_calendar_logger.exception('account_id: %s create calendar event error: %s', self.account_id, e)
            return {
                'error_msg': 'Calendar event creation failed'
            }
        finally:
            self.close()

    def update_event(self, event_data):
        try:
            return self.manager.update_event(event_data)
        except Exception as e:
            dtable_calendar_logger.exception('account_id: %s update calendar event error: %s', self.account_id, e)
            return {
                'error_msg': 'Calendar event update failed'
            }
        finally:
            self.close()

    def delete_event(self, event_data):
        try:
            return self.manager.delete_event(event_data)
        except Exception as e:
            dtable_calendar_logger.exception('account_id: %s delete calendar event error: %s', self.account_id, e)
            return {
                'error_msg': 'Calendar event deletion failed'
            }
        finally:
            self.close()

    def get_events(self, query_data):
        try:
            return self.manager.get_events(query_data)
        except Exception as e:
            dtable_calendar_logger.exception('account_id: %s get calendar events error: %s', self.account_id, e)
            return {
                'error_msg': 'Calendar events retrieval failed'
            }
        finally:
            self.close()

    def get_calendar_list(self):
        try:
            return self.manager.get_calendar_list()
        except Exception as e:
            dtable_calendar_logger.exception('account_id: %s get calendar list error: %s', self.account_id, e)
            return {
                'error_msg': 'Calendar list retrieval failed'
            }
        finally:
            self.close()

    def close(self):
        if self.is_config_session:
            self.db_session.close()


def get_google_calendar_list(account_id, detail):

    result = {}
    
    if not detail:
        result['error_msg'] = 'detail is required'
        return result
        
    try:
        calendar_api = GoogleCalendarAPI(None, account_id, detail)
        result = calendar_api.get_calendar_list()
            
    except Exception as e:
        dtable_calendar_logger.exception(f'get calendar list failure: {e}')
        result = {'error_msg': str(e)}
    
    return result
