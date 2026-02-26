import time
import base64
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email.utils import formataddr, parseaddr
from urllib import parse

import requests

from dtable_events.app.log import setup_logger
from dtable_events.automations.models import get_third_party_account, update_third_party_account_detail
from dtable_events.db import init_db_session_class
from dtable_events.statistics.db import save_email_sending_records, batch_save_email_sending_records
from abc import ABC, abstractmethod

dtable_message_logger = setup_logger('dtable_events_message', propagate=False)

class ThirdPartyAccountNotFound(Exception):
    pass

class ThirdPartyAccountInvalid(Exception):
    pass

class ThirdPartyAccountAuthorizationFailure(Exception):
    pass

class ThirdPartyAccountFetchTokenFailure(Exception):
    pass

class ThirdPartyAccountFetchEmailBoxFailure(Exception):
    pass

class InvalidEmailMessage(ValueError):
    pass

class SendEmailFailure(Exception):
    pass

def _check_and_raise_error(response):
    if response.status_code >= 400:
        raise ConnectionError(response.json())

class _SendEmailBaseClass(ABC):
    def _build_msg_obj(self, send_info, sender_name, sender_email):
        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        send_to = send_info.get('send_to', [])
        subject = send_info.get('subject', '')
        copy_to = send_info.get('copy_to', [])
        reply_to = send_info.get('reply_to', '')
        file_download_urls = send_info.get('file_download_urls', None)
        file_contents = send_info.get('file_contents', None)
        message_id = send_info.get('message_id', '')
        in_reply_to = send_info.get('in_reply_to', '')
        image_cid_url_map = send_info.get('image_cid_url_map', {})

        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        if not msg and not html_msg or not sender_name or not sender_email:
            dtable_message_logger.warning(
                'Email message invalid. message: %s, html_message: %s' % (msg, html_msg))
            raise InvalidEmailMessage('Email message invalid')

        send_to = [formataddr(parseaddr(to)) for to in send_to]
        copy_to = [formataddr(parseaddr(to)) for to in copy_to]

        # Old features compatible:
        # source = send_info.get('source', '')
        # if source:
        #    source = formataddr(parseaddr(source))

        msg_obj = MIMEMultipart()
        msg_obj['Subject'] = subject
        msg_obj['From'] = formataddr((sender_name, sender_email))
        msg_obj['To'] = ",".join(send_to)
        msg_obj['Cc'] = ",".join(copy_to)
        msg_obj['Reply-to'] = reply_to

        if message_id:
            msg_obj['Message-ID'] = message_id

        if in_reply_to:
            msg_obj['In-Reply-To'] = in_reply_to

        if msg:
            plain_content_body = MIMEText(msg)
            msg_obj.attach(plain_content_body)

        if html_msg:
            html_content_body = MIMEText(html_msg, 'html')
            msg_obj.attach(html_content_body)

        if html_msg and image_cid_url_map:
            for cid, image_url in image_cid_url_map.items():
                response = requests.get(image_url)
                msg_image = MIMEImage(response.content)
                msg_image.add_header('Content-ID', '<%s>' % cid)
                msg_obj.attach(msg_image)

        if file_download_urls:
            for file_name, file_url in file_download_urls.items():
                response = requests.get(file_url)
                attach_file = MIMEApplication(response.content)
                attach_file.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', file_name))
                msg_obj.attach(attach_file)

        if file_contents:
            for file_name, content in file_contents.items():
                if isinstance(content, str):
                    content = content.encode('utf-8')
                attach_file = MIMEApplication(content)
                attach_file.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', file_name))
                msg_obj.attach(attach_file)

        return msg_obj
    
    def _save_send_email_record(self, host, success):
        try:
            save_email_sending_records(self.db_session, self.operator, host, success)
        except Exception as e:
            dtable_message_logger.warning(
                'Email sending log record error: %s' % e)
            
    def _save_batch_send_email_record(self, host, send_state_list):
        try:
            batch_save_email_sending_records(self.db_session, self.operator, host, send_state_list)
        except Exception as e:
            dtable_message_logger.warning('Batch save email sending log error: %s' % e)

    @abstractmethod
    def send(self, send_info): ...

    @abstractmethod
    def batch_send(self,send_info_list): ...

class SMTPSendEmail(_SendEmailBaseClass):

    def __init__(self, db_session, account_id, detail, operator):
        self.db_session = db_session
        self.account_id = account_id
        self.email_host = detail.get('email_host')
        self.email_port = detail.get('email_port')
        self.host_user = detail.get('host_user')
        self.password = detail.get('password')
        self.sender_name = detail.get('sender_name')
        self.sender_email = detail.get('sender_email')
        self.operator = operator

        if not all([self.email_host,  self.email_port, self.host_user, self.password]):
            dtable_message_logger.exception('Third party account %s is invalid.' % self.account_id)
            raise ThirdPartyAccountInvalid() 
    
    def send(self, send_info):
        copy_to = send_info.get('copy_to', [])
        send_to = send_info.get('send_to', [])
        
        try:
            msg_obj = self._build_msg_obj(send_info, self.sender_name, self.sender_email or self.host_user)
        except Exception as e:
            dtable_message_logger.exception(f'Build MIME object failure: {e}')
            raise InvalidEmailMessage()
        
        try:
            smtp = smtplib.SMTP(self.email_host, int(self.email_port), timeout=30)
            smtp.starttls()
            smtp.login(self.host_user, self.password)
        except Exception as e:
            dtable_message_logger.exception(
                'Email server authorization failed. host: %s, port: %s, error: %s' % (self.email_host, self.email_port, e))
            raise ThirdPartyAccountAuthorizationFailure()
        
        success = False
        try:
            recevers = copy_to and send_to + copy_to or send_to
            smtp.sendmail(self.sender_email if self.sender_email else self.host_user, recevers, msg_obj.as_string())
            success = True
        except Exception as e:
            dtable_message_logger.exception(
                'Send email failure: email: %s, error: %s' % (self.host_user, e))
        else:
            dtable_message_logger.info('Email sending success!')
        finally:
            smtp.quit()

        self._save_send_email_record(self.email_host, success)

        if not success:
            raise SendEmailFailure()

        return {'success': True}

    def batch_send(self, send_info_list):
        try:
            smtp = smtplib.SMTP(self.email_host, int(self.email_port), timeout=30)
            smtp.starttls()
            smtp.login(self.host_user, self.password)
        except Exception as e:
            dtable_message_logger.exception(
                'Email server authorization failed. host: %s, port: %s, error: %s' % (self.email_host, self.email_port, e))
            raise ThirdPartyAccountAuthorizationFailure()

        send_state_list = []
        for send_info in send_info_list:
            success = False
            send_to = send_info.get('send_to', [])
            copy_to = send_info.get('copy_to', [])

            try:
                msg_obj = self._build_msg_obj(send_info, self.sender_name, self.sender_email or self.host_user)
            except Exception as e:
                dtable_message_logger.warning(f'Batch send emails: Build MIME object failure: {e}')
                continue

            send_to = [formataddr(parseaddr(to)) for to in send_to]
            copy_to = [formataddr(parseaddr(to)) for to in copy_to]

            try:
                recevers = copy_to and send_to + copy_to or send_to
                smtp.sendmail(self.sender_email if self.sender_email else self.host_user, recevers, msg_obj.as_string())
                success = True
            except Exception as e:
                dtable_message_logger.warning('Batch send emails: email sending failed. email: %s, error: %s' % (self.host_user, e))
            send_state_list.append(success)
            time.sleep(0.5)

        smtp.quit()
        self._save_batch_send_email_record(self.email_host, send_state_list)

class _ThirdpartyAPISendEmail(_SendEmailBaseClass):

    def __init__(self, db_session, account_id, detail, operator):
        self.account_id = account_id
        self.detail = detail
        self.db_session = db_session
        self.client_id = detail.get('client_id')
        self.client_secret = detail.get('client_secret')
        self.refresh_token = detail.get('refresh_token')
        self.access_token = detail.get('access_token')
        self.expires_at = detail.get('expires_at')
        self.token_url = detail.get('token_url')
        self.scopes = detail.get('scopes')
        self.operator = operator

        # compatible for the accounts in 6.0.x 
        self.sender_name = detail.get('sender_name')
        self.sender_email = detail.get('sender_email')

        if not all([self.client_id,  self.client_secret, self.refresh_token, self.token_url, self.scopes, self.operator]):
            dtable_message_logger.exception('Third party account %s is invalid.' % self.account_id)
            raise ThirdPartyAccountInvalid()
        
        self._request_access_token()
        
    # request access_token
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
            try:
                _check_and_raise_error(response)
            except Exception as e:
                dtable_message_logger.exception(f'Failure to fetch new access token, account_id: {self.account_id}, reason: {e}')
                raise ThirdPartyAccountFetchTokenFailure()
            else:
                response = response.json()
                if 'access_token' not in response:
                    dtable_message_logger.exception(f'Failure to fetch new access token, account_id: {self.account_id}. There is not available access_token in response.')
                    raise ThirdPartyAccountAuthorizationFailure()
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
        update_third_party_account_detail(self.db_session, self.account_id, self.detail)

    @abstractmethod
    def _on_sending_email(self, /): ...

    @abstractmethod
    def _get_sender(self, /): ...

    def send(self, send_info):
        if not self.sender_name and not self.sender_email:
            sender_name, sender_email = self._get_sender()
        else:
            # compatible for 6.0.x
            sender_name = self.sender_name
            sender_email = self.sender_email

        try:
            msg_obj = self._build_msg_obj(send_info, sender_name, sender_email)
        except Exception as e:
            dtable_message_logger.exception(f'Build MIME object failure: {e}')
            raise InvalidEmailMessage()
        
        response = self._on_sending_email(msg_obj)

        success = False
        exception = None
        try:
            _check_and_raise_error(response)
            success = True
        except Exception as e:
            exception = e
            dtable_message_logger.exception(
                f'Email sending failed, error: {e}')
        
        self._save_send_email_record('<OAuth authenticated account>', success)
        if not success:
            raise SendEmailFailure(repr(exception))
        
        return {'success': True}

    def batch_send(self, send_info_list):
        if not self.sender_name and not self.sender_email:
            sender_name, sender_email = self._get_sender()
        else:
            # compatible for 6.0.x
            sender_name = self.sender_name
            sender_email = self.sender_email

        send_state_list = []
        for send_info in send_info_list:
            success = False

            try:
                msg_obj = self._build_msg_obj(send_info, sender_name, sender_email)
            except Exception as e:
                dtable_message_logger.warning(f'Batch send emails: Build MIME object failure: {e}')
                continue

            response = self._on_sending_email(msg_obj)

            try:
                _check_and_raise_error(response)
                success = True
            except Exception as e:
                dtable_message_logger.warning(f'Batch send emails: Email sending failed, error: {e}')

            send_state_list.append(success)
            time.sleep(0.5)

        self._save_batch_send_email_record('<OAuth authenticated account>', send_state_list)
    
class GoogleAPISendEmail(_ThirdpartyAPISendEmail):
    def __init__(self, db_session, account_id, detail, operator):
        super().__init__(db_session, account_id, detail, operator)

        self.gmail_api_get_sender_endpoint = f'https://gmail.googleapis.com/gmail/v1/users/me/settings/sendAs'
        self.gmail_api_send_emails_endpoint = f'https://gmail.googleapis.com/upload/gmail/v1/users/me/messages/send?uploadType=multipart'

    def _get_sender(self):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.get(self.gmail_api_get_sender_endpoint, headers=headers)
        try:
            _check_and_raise_error(response)
            sender_info = response.json()['sendAs'][0]
            sender_name = sender_info['displayName']
            sender_email = sender_info['sendAsEmail']
        except Exception as e:
            dtable_message_logger.exception(f'Failure to fetch sender info: {e}')
            raise ThirdPartyAccountFetchEmailBoxFailure(repr(e))
        return sender_name, sender_email

    def _on_sending_email(self, msg_obj):
        msg_string = msg_obj.as_string()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'message/rfc822'
        }
        
        return requests.post(self.gmail_api_send_emails_endpoint, data=msg_string, headers=headers)

class MicrosoftAPISendEmail(_ThirdpartyAPISendEmail):
    def __init__(self, db_session, account_id, detail, operator):
        super().__init__(db_session, account_id, detail, operator)

        self.microsoft_api_get_sender_endpoint = 'https://graph.microsoft.com/v1.0/me'
        self.microsoft_api_send_emails_endpoint = 'https://graph.microsoft.com/v1.0/me/sendMail'

    def _get_sender(self):
        headers = {
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.get(self.microsoft_api_get_sender_endpoint, headers=headers)
        try:
            _check_and_raise_error(response)
            response = response.json()
            sender_name = response['displayName']
            sender_email = response['mail']
        except Exception as e:
            dtable_message_logger.exception(f'Failure to fetch sender info: {e}')
            raise ThirdPartyAccountFetchEmailBoxFailure(repr(e))
        return sender_name, sender_email

    def _on_sending_email(self, msg_obj):
        msg_bytes = msg_obj.as_bytes()
        msg_base64 = base64.b64encode(msg_bytes).decode()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'text/plain'
        }
        
        return requests.post(self.microsoft_api_send_emails_endpoint, data=msg_base64, headers=headers)

class EmailSender:
    def __init__(self, account_id, operator, config=None, db_session=None):
        """
        one of config or db_session is required
        """
        self.account_id = account_id
        self.operator = operator
        self.config = config
        self.is_config_session = False if db_session else True
        self.db_session = db_session or init_db_session_class(self.config)()
        self._sender_init()

    def _sender_init(self):
        account_info = get_third_party_account(self.db_session, self.account_id)
        if not account_info:
            dtable_message_logger.exception('Third party account %s does not exists.' % self.account_id)
            raise ThirdPartyAccountNotFound()
        detail = account_info.get('detail')
        if account_info.get('account_type') != 'email' or not detail:
            dtable_message_logger.exception('Third party account %s is invalid.' % self.account_id)
            raise ThirdPartyAccountInvalid()

        self.email_provider = detail.get('email_provider', 'GeneralEmailProvider')

        if self.email_provider == 'GeneralEmailProvider':
            self.sender = SMTPSendEmail(self.db_session, self.account_id, detail, self.operator)
        elif self.email_provider == 'Gmail':
            self.sender = GoogleAPISendEmail(self.db_session, self.account_id, detail, self.operator)
        elif self.email_provider in ('Microsoft', 'Outlook'):
            self.sender = MicrosoftAPISendEmail(self.db_session, self.account_id, detail, self.operator)
        else:
            dtable_message_logger.exception(f'Invalid third party account type: account_id: {self.account_id} account type: {self.email_provider}')
            raise ThirdPartyAccountInvalid()
        
    def send(self, send_info):
        exception = None
        try:
            return self.sender.send(send_info)
        except Exception as e:
            exception = e
        finally:
            self.close()

        if exception:
            raise exception

    def batch_send(self, send_info_list):
        exception = None
        try:
            return self.sender.batch_send(send_info_list)
        except Exception as e:
            exception = e
        finally:
            self.close()
        
        if exception:
            raise exception

    def close(self):
        # only for db_session = None in __init__
        if self.is_config_session:
            self.db_session.close()

def toggle_send_email(account_id, send_info, username, config):
    return EmailSender(account_id, username, config).send(send_info)
