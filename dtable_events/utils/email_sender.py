import json
import time
import base64
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from urllib import parse

import requests

from dtable_events.app.log import setup_logger
from dtable_events.automations.models import get_third_party_account, update_third_party_account_detail
from dtable_events.db import init_db_session_class
from dtable_events.statistics.db import save_email_sending_records, batch_save_email_sending_records
from abc import ABC, abstractmethod

dtable_message_logger = setup_logger('dtable_events_message.log')

class ThirdPartyAccountNotFound(Exception):
    pass

class ThirdPartyAccountInvalid(Exception):
    pass

def _check_and_raise_error(response):
    if response.status_code >= 400:
        raise ConnectionError(response.json())

class _SendEmailBaseClass(ABC):
    def _build_msg_obj(self, send_info):
        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        send_to = send_info.get('send_to', [])
        subject = send_info.get('subject', '')
        source = send_info.get('source', '')
        copy_to = send_info.get('copy_to', [])
        reply_to = send_info.get('reply_to', '')
        file_download_urls = send_info.get('file_download_urls', None)
        file_contents = send_info.get('file_contents', None)
        message_id = send_info.get('message_id', '')
        in_reply_to = send_info.get('in_reply_to', '')
        image_cid_url_map = send_info.get('image_cid_url_map', {})

        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        if not msg and not html_msg:
            dtable_message_logger.warning(
                'Email message invalid. message: %s, html_message: %s' % (msg, html_msg))
            raise ValueError('Email message invalid')

        send_to = [formataddr(parseaddr(to)) for to in send_to]
        copy_to = [formataddr(parseaddr(to)) for to in copy_to]
        if source:
            source = formataddr(parseaddr(source))

        msg_obj = MIMEMultipart()
        msg_obj['Subject'] = subject
        msg_obj['From'] = source or formataddr((self.sender_name, self.sender_email if self.sender_email else self.host_user))
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
                from email.mime.image import MIMEImage
                msg_image = MIMEImage(response.content)
                msg_image.add_header('Content-ID', '<%s>' % cid)
                msg_obj.attach(msg_image)

        if file_download_urls:
            for file_name, file_url in file_download_urls.items():
                response = requests.get(file_url)
                attach_file = MIMEText(response.content, 'base64', 'utf-8')
                attach_file["Content-Type"] = 'application/octet-stream'
                attach_file["Content-Disposition"] = 'attachment;filename*=UTF-8\'\'' + parse.quote(file_name)
                msg_obj.attach(attach_file)

        if file_contents:
            for file_name, content in file_contents.items():
                attach_file = MIMEText(content, 'base64', 'utf-8')
                attach_file["Content-Type"] = 'application/octet-stream'
                attach_file["Content-Disposition"] = 'attachment;filename*=UTF-8\'\'' + parse.quote(file_name)
                msg_obj.attach(attach_file)

        return msg_obj
    
    def _save_send_email_record(self, host, success):
        try:
            save_email_sending_records(self.db_session, self.operator, host, success)
        except Exception as e:
            dtable_message_logger.exception(
                'Email sending log record error: %s' % e)
            
    def _save_batch_send_email_record(self, host, send_state_list):
        try:
            batch_save_email_sending_records(self.db_session, self.operator, host, send_state_list)
        except Exception as e:
            dtable_message_logger.exception('Batch save email sending log error: %s' % e)

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
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id) 
    
    def send(self, send_info):
        copy_to = send_info.get('copy_to', [])
        send_to = send_info.get('send_to', [])
        
        result = {}
        try:
            msg_obj = self._build_msg_obj(send_info)
        except Exception as e:
            result['err_msg'] = 'Email message invalid'
            return result

        try:
            smtp = smtplib.SMTP(self.email_host, int(self.email_port), timeout=30)
        except Exception as e:
            dtable_message_logger.warning(
                'Email server configured failed. host: %s, port: %s, error: %s' % (self.email_host, self.email_port, e))
            result['err_msg'] = 'Email server host or port invalid'
            return result
        success = False

        try:
            smtp.starttls()
            smtp.login(self.host_user, self.password)
            recevers = copy_to and send_to + copy_to or send_to
            smtp.sendmail(self.sender_email if self.sender_email else self.host_user, recevers, msg_obj.as_string())
            success = True
        except Exception as e:
            dtable_message_logger.warning(
                'Email sending failed. email: %s, error: %s' % (self.host_user, e))
            result['err_msg'] = 'Email server username or password invalid'
        else:
            dtable_message_logger.info('Email sending success!')
        finally:
            smtp.quit()

        self._save_send_email_record(self.email_host, success)

        return result

    def batch_send(self, send_info_list):
        try:
            smtp = smtplib.SMTP(self.email_host, int(self.email_port), timeout=30)
        except Exception as e:
            dtable_message_logger.warning(
                'Email server configured failed. host: %s, port: %s, error: %s' % (self.email_host, self.email_port, e))
            return

        try:
            smtp.starttls()
            smtp.login(self.host_user, self.password)
        except Exception as e:
            dtable_message_logger.warning(
                'Login smtp failed, host user: %s, error: %s' % (self.host_user, e))
            return

        send_state_list = []
        for send_info in send_info_list:
            success = False
            send_to = send_info.get('send_to', [])
            copy_to = send_info.get('copy_to', [])


            try:
                msg_obj = self._build_msg_obj(send_info)
            except Exception as e:
                dtable_message_logger.warning('Email message invalid')
                continue

            send_to = [formataddr(parseaddr(to)) for to in send_to]
            copy_to = [formataddr(parseaddr(to)) for to in copy_to]

            try:
                recevers = copy_to and send_to + copy_to or send_to
                smtp.sendmail(self.sender_email if self.sender_email else self.host_user, recevers, msg_obj.as_string())
                success = True
            except Exception as e:
                dtable_message_logger.warning('Email sending failed. email: %s, error: %s' % (self.host_user, e))
            else:
                dtable_message_logger.info('Email sending success!')
            send_state_list.append(success)
            time.sleep(0.5)

        smtp.quit()

        self._save_batch_send_email_record(self.email_host, send_state_list)

class _ThirdpartyAPISendEmail(_SendEmailBaseClass):

    def __init__(self, db_session, account_id, detail, operator):
        self.account_id = account_id
        self.detail = detail
        self.db_session = db_session
        self.host_user = detail.get('host_user')
        self.sender_name = detail.get('sender_name')
        self.sender_email = detail.get('sender_email')
        self.client_id = detail.get('client_id')
        self.client_secret = detail.get('client_secret')
        self.refresh_token = detail.get('refresh_token')
        self.access_token = detail.get('access_token')
        self.expires_at = detail.get('expires_at')
        self.token_url = detail.get('token_url')
        self.scopes = detail.get('scopes')
        self.operator = operator

        if not all([self.client_id,  self.client_secret, self.refresh_token, self.host_user, self.token_url, self.scopes, self.operator]):
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id)
        
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
            _check_and_raise_error(response)
            response = response.json()
            dtable_message_logger.exception(response)
            self._check_and_update_refresh_token(response.get('access_token'), response.get('ext_expires_in') + time.time(), response.get('refresh_token'))

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

    def send(self, send_info):
        result = {}
        
        try:
            msg_obj = self._build_msg_obj(send_info)
        except Exception as e:
            result['err_msg'] = 'Email message invalid'
            dtable_message_logger.exception(e)
            return result
        
        response = self._on_sending_email(msg_obj)

        success = False
        try:
            _check_and_raise_error(response)
            success = True
        except Exception as e:
            dtable_message_logger.warning(
                'Email sending failed. email: %s, error: %s' % (self.host_user, e))
            result['err_msg'] = 'Email server username or password invalid'
        else:
            dtable_message_logger.info(f'Email sending success: ({response.status_code}) {response.text}')
        
        self._save_send_email_record(self.host_user, success)

    def batch_send(self, send_info_list):
        send_state_list = []
        for send_info in send_info_list:
            success = False

            try:
                msg_obj = self._build_msg_obj(send_info)
            except Exception as e:
                dtable_message_logger.warning('Email message invalid')
                continue

            response = self._on_sending_email(msg_obj)

            try:
                _check_and_raise_error(response)
                success = True
            except Exception as e:
                dtable_message_logger.warning('Email sending failed. email: %s, error: %s' % (self.host_user, e))
            else:
                dtable_message_logger.info(f'Email sending success: ({response.status_code}) {response.text}')

            send_state_list.append(success)
            time.sleep(0.5)

        self._save_batch_send_email_record(self.host_user, send_state_list)
    
class GoogleAPISendEmail(_ThirdpartyAPISendEmail):
    def __init__(self, db_session, account_id, detail, operator):
        super().__init__(db_session, account_id, detail, operator)

        self.gmail_api_send_emails_endpoint = f'https://gmail.googleapis.com/upload/gmail/v1/users/{parse.quote(self.host_user)}/messages/send?uploadType=multipart'

    def _on_sending_email(self, msg_obj):
        msg_string = msg_obj.as_string()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'message/rfc822'
        }
        
        return requests.post(self.gmail_api_send_emails_endpoint, data=msg_string, headers=headers)

class MS365APISendEmail(_ThirdpartyAPISendEmail):
    def __init__(self, db_session, account_id, detail, operator):
        super().__init__(db_session, account_id, detail, operator)

        self.outlook_api_send_emails_endpoint = 'https://graph.microsoft.com/v1.0' + ('/me/sendMail' if self.host_user == 'me' else f'/users/{parse.quote(self.host_user)}/sendMail')

    def _build_msg_obj_ms_json(self, send_info):
        # send info
        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        send_to = send_info.get('send_to', [])
        subject = send_info.get('subject', '')
        copy_to = send_info.get('copy_to', [])
        reply_to = send_info.get('reply_to') or []
        file_download_urls = send_info.get('file_download_urls', None)
        file_contents = send_info.get('file_contents', None)
        message_id = send_info.get('message_id', '')

        if not msg and not html_msg:
            dtable_message_logger.warning(
                'Email message invalid. message: %s, html_message: %s' % (msg, html_msg))
            raise ValueError('Email message invalid')
        
        email_data = {
            'message':{
                'subject': subject,
            },
            'body': {
                'contentType': 'html' if html_msg else 'text',
                'content': html_msg if html_msg else msg
            },
            'from':{
                'emailAddress': {
                    'address': self.sender_email if self.sender_name else self.host_user
                }
            },
            'toRecipients': [
                {
                    'emailAddress':{
                        'address': to
                    }
                }
                for to in send_to
            ],
            'ccRecipients': [
                {
                    'emailAddress':{
                        'address': to
                    }
                }
                for to in copy_to
            ],
            'replyTo':[
                {
                    'emailAddress':{
                        'address': to
                    }
                }
                for to in reply_to
            ]
        }

        if self.sender_name:
            email_data['from']['emailAddress']['name'] = self.sender_name

        if message_id:
            email_data.update({'internetMessageId': message_id})

        attachments = []

        if file_download_urls:
            attachments.extend([
                {
                    '@odata.type': '#microsoft.graph.fileAttachment',
                    'name': 'UTF-8\'\'' + parse.quote(file_name),
                    'contentType': 'application/octet-stream',
                    'contentBytes': requests.get(file_url).content
                }
                for file_name, file_url in file_download_urls.items()
            ])

        if file_contents:
            attachments.extend([
                {
                    '@odata.type': '#microsoft.graph.fileAttachment',
                    'name': 'UTF-8\'\'' + parse.quote(file_name),
                    'contentType': 'application/octet-stream',
                    'contentBytes': file_content
                }
                for file_name, file_content in file_contents.items()
            ])

        if attachments:
            email_data.update({'attachments': attachments})

        return email_data
    
    def _on_sending_email_by_ms_json(self, email_data):
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }

        return requests.post(
            self.outlook_api_send_emails_endpoint,
            data=json.dumps(email_data),
            headers=headers
        )

    def _on_sending_email(self, msg_obj):
        msg_bytes = msg_obj.as_bytes()
        msg_base64 = base64.b64encode(msg_bytes).decode()

        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'text/plain'
        }
        
        return requests.post(self.outlook_api_send_emails_endpoint, data=msg_base64, headers=headers)

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
            raise ThirdPartyAccountNotFound('Third party account %s does not exists.' % self.account_id)
        detail = account_info.get('detail')
        if account_info.get('account_type') != 'email' or not detail:
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id)

        self.email_provider = detail.get('email_provider', 'GeneralEmailProvider')

        if self.email_provider == 'GeneralEmailProvider':
            self.sender = SMTPSendEmail(self.db_session, self.account_id, detail, self.operator)
        elif self.email_provider == 'Gmail':
            self.sender = GoogleAPISendEmail(self.db_session, self.account_id, detail, self.operator)
        elif self.email_provider == 'Outlook':
            self.sender = MS365APISendEmail(self.db_session, self.account_id, detail, self.operator)
        else:
            raise ThirdPartyAccountInvalid(f'Invalid third-party email-account account_id: {self.account_id} account type: {self.email_provider}')
        
    def send(self, send_info):
        try:
            return self.sender.send(send_info)
        except Exception as e:
            dtable_message_logger.exception('account_id: %s send email error: %s', self.account_id, e)
            return {
                'error_msg': 'Email send failed'
            }
        finally:
            self.close()

    def batch_send(self, send_info_list):
        try:
            return self.sender.batch_send(send_info_list)
        except Exception as e:
            dtable_message_logger.exception('account_id: %s batch send email error: %s', self.account_id, e)
        finally:
            self.close()

    def close(self):
        # only for db_session = None in __init__
        if self.is_config_session:
            self.db_session.close()

def toggle_send_email(account_id, send_info, username, config):
    result = {}
    try:
        sender = EmailSender(account_id, username, config)
        result = sender.send(send_info)
    except Exception as e:
        dtable_message_logger.exception(f'toggle send email failure: {e}')
        result['error_msg'] = e
    return result
