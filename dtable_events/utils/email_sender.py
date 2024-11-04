import json
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr, parseaddr
from urllib import parse

import msal
import requests

from dtable_events.app.log import setup_logger
from dtable_events.automations.models import get_third_party_account
from dtable_events.db import init_db_session_class
from dtable_events.statistics.db import save_email_sending_records, batch_save_email_sending_records

dtable_message_logger = setup_logger('dtable_events_message.log')

class ThirdPartyAccountNotFound(BaseException):
    pass

class ThirdPartyAccountInvalid(BaseException):
    pass

class EmailSender:
    def __init__(self, account_id, config=None, db_session=None):
        """
        one of config or db_session is required
        """
        self.account_id = account_id
        self.config = config
        self.is_config_session = False if db_session else True
        self.db_session = db_session or init_db_session_class(self.config)()
        self._get_auth_info()

    def send(self, send_info, username):
        try:
            return self._send_smtp_email(send_info, username) if self.account_type == 'LOGIN' else self._send_oauth_email(send_info, username)
        except Exception as e:
            dtable_message_logger.exception('account_id: %s send smtp email error: %s', self.account_id, e)
            return {
                'error_msg': 'Email send failed'
            }
        finally:
            self.close()

    def batch_send(self, send_info_list, username):
        try:
            return self._batch_send_smtp_email(send_info_list, username) if self.account_type == 'LOGIN' else self._batch_send_oauth_email(send_info_list, username)
        except Exception as e:
            dtable_message_logger.exception('account_id: %s batch send smtp email error: %s', self.account_id, e)
        finally:
            self.close()

    def close(self):
        # only for db_session = None in __init__
        if self.is_config_session:
            self.db_session.close()

    def _get_auth_info(self):
        account_info = get_third_party_account(self.db_session, self.account_id)
        if not account_info:
            raise ThirdPartyAccountNotFound('Third party account %s does not exists.' % self.account_id)
        detail = account_info.get('detail')
        if account_info.get('account_type') != 'email' or not detail:
            raise ThirdPartyAccountInvalid('Third party account %s is invalid.' % self.account_id)

        self.account_type = detail.get('type', 'LOGIN')
        if self.account_type == 'LOGIN':
            self.email_host = detail.get('email_host')
            self.email_port = detail.get('email_port')
            self.host_user = detail.get('host_user')
            self.password = detail.get('password')
            self.sender_name = detail.get('sender_name')
            self.sender_email = detail.get('sender_email')
        elif self.account_type == 'OAUTH':
            self.host_user = detail.get('host_user')
            self.client_id = detail.get('client_id')
            self.client_secret = detail.get('client_secret')
            self.authority = detail.get('authority')
            self.scopes = detail.get('scopes')
            self.endpoint = detail.get('endpoint')
            self.sender_name = detail.get('sender_name')
            self.sender_email = detail.get('sender_email')
        else:
            raise ThirdPartyAccountInvalid(f'Invalid third-party email-account account_id: {self.account_id} account type: {self.account_type}')
        
    def _get_oauth_smtp_headers(self):
        app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority,
            client_credential=self.client_secret,
        )
        result = app.acquire_token_for_client(scopes=self.scopes.replace(' ','').split(','))
        if "access_token" in result:
            headers = {
                'Authorization': f'Bearer {result["access_token"]}',
                'Content-Type': 'application/json'
            }
            return headers

        else:
            raise ConnectionError(f'Failed to acquire token account_id: {self.account_id}', result.get('error'), result.get('error_description'))

    def _send_smtp_email(self, send_info, username):
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

        send_to = [formataddr(parseaddr(to)) for to in send_to]
        copy_to = [formataddr(parseaddr(to)) for to in copy_to]
        if source:
            source = formataddr(parseaddr(source))

        result = {}
        if not msg and not html_msg:
            result['err_msg'] = 'Email message invalid'
            return result

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

        session = self.db_session
        try:
            save_email_sending_records(session, username, self.email_host, success)
        except Exception as e:
            dtable_message_logger.error(
                'Email sending log record error: %s' % e)
        return result

    def _batch_send_smtp_email(self, send_info_list, username):
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
            msg = send_info.get('message', '')
            html_msg = send_info.get('html_message', '')
            send_to = send_info.get('send_to', [])
            subject = send_info.get('subject', '')
            source = send_info.get('source', '')
            copy_to = send_info.get('copy_to', [])
            reply_to = send_info.get('reply_to', '')
            file_download_urls = send_info.get('file_download_urls', None)
            file_contents = send_info.get('file_contents', None)
            image_cid_url_map = send_info.get('image_cid_url_map', {})

            if not msg and not html_msg:
                dtable_message_logger.warning('Email message invalid')
                continue

            send_to = [formataddr(parseaddr(to)) for to in send_to]
            copy_to = [formataddr(parseaddr(to)) for to in copy_to]

            msg_obj = MIMEMultipart()
            msg_obj['Subject'] = subject
            msg_obj['From'] = source or formataddr((self.sender_name, self.sender_email if self.sender_email else self.host_user))
            msg_obj['To'] = ",".join(send_to)
            msg_obj['Cc'] = copy_to and ",".join(copy_to) or ""
            msg_obj['Reply-to'] = reply_to

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

        session = self.db_session
        try:
            batch_save_email_sending_records(session, username, self.email_host, send_state_list)
        except Exception as e:
            dtable_message_logger.error('Batch save email sending log error: %s' % e)

    def _send_oauth_email(self, send_info, username):
        # send info
        msg = send_info.get('message', '')
        html_msg = send_info.get('html_message', '')
        send_to = send_info.get('send_to', [])
        subject = send_info.get('subject', '')
        copy_to = send_info.get('copy_to', [])
        reply_to = send_info.get('reply_to', '')
        file_download_urls = send_info.get('file_download_urls', None)
        file_contents = send_info.get('file_contents', None)
        message_id = send_info.get('message_id', '')

        result = {}
        if not msg and not html_msg:
            result['err_msg'] = 'Email message invalid'
            return result
        
        try:
            headers = self._get_oauth_smtp_headers()
        except Exception as e:
            dtable_message_logger.warning(f'SMTP auth failure: {e}')
            result['err_msg'] = 'SMTP auth failure'
            return result
        
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

        try:
            response = requests.post(
                self.endpoint,
                headers=headers,
                data=json.dumps(email_data)
            )
            if response.status_code != 202:
                raise Exception(f'Error sending email: {response.status_code} - {response.text}')
            success = True
        except Exception as e:
            dtable_message_logger.warning(e)
            result['err_msg'] = e
        else:
            dtable_message_logger.info('Email sending success!')

        session = self.db_session
        try:
            save_email_sending_records(session, username, self.endpoint, success)
        except Exception as e:
            dtable_message_logger.error(
                'Email sending log record error: %s' % e)
        return result

    def _batch_send_oauth_email(self, send_info_list, username):
            try:
                headers = self._get_oauth_smtp_headers()
            except Exception as e:
                dtable_message_logger.warning(f'SMTP auth failure: {e}')
                return
            
            send_state_list = []
            for send_info in send_info_list:
                success = False
                # send info
                msg = send_info.get('message', '')
                html_msg = send_info.get('html_message', '')
                send_to = send_info.get('send_to', [])
                subject = send_info.get('subject', '')
                copy_to = send_info.get('copy_to', [])
                reply_to = send_info.get('reply_to', '')
                file_download_urls = send_info.get('file_download_urls', None)
                file_contents = send_info.get('file_contents', None)
                message_id = send_info.get('message_id', '')
                
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

                try:
                    response = requests.post(
                        self.endpoint,
                        headers=headers,
                        data=json.dumps(email_data)
                    )
                    if response.status_code != 202:
                        raise Exception(f'Error sending email: {response.status_code} - {response.text}')
                    success = True
                except Exception as e:
                    dtable_message_logger.warning(e)
                else:
                    dtable_message_logger.info('Email sending success!')
                send_state_list.append(success)
                time.sleep(0.5)

            session = self.db_session
            try:
                batch_save_email_sending_records(session, username, self.endpoint, success)
            except Exception as e:
                dtable_message_logger.error(
                    'Email sending log record error: %s' % e)

def toggle_send_email(account_id, send_info, username, config):
    sender = EmailSender(account_id, config)
    result = sender.send(send_info, username)
    return result
