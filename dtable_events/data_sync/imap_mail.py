import logging
from imapclient import IMAPClient
from email.parser import Parser
from email.header import decode_header
from datetime import datetime, timedelta
from email.utils import parseaddr, parsedate_to_datetime
from tzlocal import get_localzone

logger = logging.getLogger(__name__)


class ImapMail(object):
    def __init__(self, serveraddress, user, passwd, port=None, timeout=None, ssl_context=None):
        self.serveraddress = serveraddress
        self.user = user
        self.passwd = passwd
        self.port = port
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.server = None

    def client(self):
        self.server = IMAPClient(self.serveraddress, self.port, timeout=self.timeout, ssl_context=self.ssl_context)
        self.server._imap.debug = 0
        if '163.com' in self.serveraddress:
            self.server.id_({"name": "IMAPClient", "version": "2.1.0"})
        logger.info('connected success')

    def login(self):
        self.server.login(self.user, self.passwd)

    @staticmethod
    def decode_str(s):
        value, charset = decode_header(s)[0]
        if not charset:
            return value
        if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
            charset = 'gb18030'
        return value.decode(charset)

    @staticmethod
    def parse_content(part):
        content = ''
        charset = part.get_content_charset()
        if charset:
            if charset.lower() in ['gb2312', 'gbk', 'gb18030']:
                charset = 'gb18030'
            try:
                content = part.get_payload(decode=True).decode(charset)
            except LookupError:
                content = part.get_payload()
                logger.info('unknown encoding: %s' % charset)
            except UnicodeDecodeError:
                content = part.get_payload()
                logger.info('%s can\'t decode unicode' % charset)
            except Exception as e:
                logger.error(e)
        else:
            content = part.get_payload()
        return content

    def get_content(self, msg):
        plain_content = ''
        html_content = ''
        for part in msg.walk():
            if part.is_multipart():
                continue
            # if attachment continue
            if part.get_filename() is not None:
                continue

            content_type = part.get_content_type()
            if content_type == 'text/plain':
                plain_content = self.parse_content(part)
            elif content_type == 'text/html':
                html_content = '```' + self.parse_content(part) + '```'

        return plain_content, html_content

    def get_attachments(self, msg):
        file_list = []
        content_info = {}
        for part in msg.walk():
            content_id = part.get('Content-ID')
            filename = part.get_filename()
            content_type = part.get_content_type()

            # filename maybe empty if content-type is image
            if filename is not None or (content_id is not None and 'image' in content_type):
                data = part.get_payload(decode=True)
                filename = self.decode_str(filename) if filename else content_id[1:-1] + '.' + content_type[6:]
                file_list.append({'file_name': filename, 'file_data': data})

            if content_id is not None and 'image' in content_type:
                content_info[filename] = content_id[1:-1]
        return file_list, content_info

    def parse_addr(self, addr):
        addr_info = parseaddr(addr)
        nickname = self.decode_str(addr_info[0])
        if not nickname:
            nickname = addr_info[1].split('@')[0]
        return nickname + ' <' + addr_info[1] + '>'

    def get_email_header(self, msg):
        header_info = {}
        for header in ['From', 'To', 'CC', 'Subject', 'Message-ID', 'In-Reply-To', 'Date']:
            value = msg.get(header, '')
            if value:
                if header == 'Subject':
                    value = self.decode_str(value)
                elif header == 'Date':
                    value = parsedate_to_datetime(value).astimezone(get_localzone()).isoformat().replace('T', ' ')[:-6]
                elif header in ['From', 'To', 'CC']:
                    value = ','.join([self.parse_addr(val) for val in value.split(',')])
                elif header in ['Message-ID', 'In-Reply-To']:
                    value = value.strip()
            header_info[header] = value

        return header_info

    def gen_email_dict(self, mail, mode, send_box, send_date=None):
        email_dict = {}
        msg_dict = self.server.fetch(mail, ['BODY[]'])
        mail_body = msg_dict[mail][b'BODY[]']
        try:
            mail_body = mail_body.decode()
        except UnicodeDecodeError:
            mail_body = mail_body.decode('gb18030')
        msg = Parser().parsestr(mail_body)

        header_info = self.get_email_header(msg)
        send_time = header_info.get('Date')
        try:
            send_time = datetime.strptime(send_time, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            logger.error('parse send_time error: %s', e)
            send_time = datetime.today().date()

        if mode == 'ON' and send_time.date() != send_date:
            return
        if mode == 'SINCE' and send_time.date() < send_date:
            return

        if not header_info.get('Message-ID'):
            logger.error('message-id not found, email body: %s', mail_body)

        if not header_info['From']:
            logger.warning('account: %s message: %s no sender!', self.user, mail)
        if not header_info['To']:
            logger.warning('account: %s message: %s no recipient!', self.user, mail)
        plain_content, html_content = self.get_content(msg)
        file_list, content_info = self.get_attachments(msg)
        email_dict['Content'] = plain_content
        email_dict['HTML Content'] = html_content
        email_dict['filename2content_id'] = content_info
        email_dict['Attachment'] = file_list
        email_dict['From'] = header_info.get('From')
        email_dict['To'] = header_info.get('To')
        email_dict['Subject'] = header_info.get('Subject')
        email_dict['Message ID'] = header_info.get('Message-ID')
        email_dict['Reply to Message ID'] = header_info.get('In-Reply-To')
        email_dict['cc'] = header_info.get('CC')
        email_dict['Date'] = header_info.get('Date')
        email_dict['is_sender'] = True if send_box == 'Sent Items' else False

        return email_dict

    def search_emails_by_send_date(self, send_date, mode='ON'):
        send_date = datetime.strptime(send_date, '%Y-%m-%d').date()
        td = timedelta(days=1)
        before_send_date = send_date - td
        after_send_date = send_date + td
        total_email_list = []
        for send_box in ['INBOX', 'Sent Items']:
            logger.debug('start to get user: %s emails from box: %s', self.user, send_box)
            try:
                self.server.select_folder(send_box, readonly=True)
            except Exception as e:
                logger.warning('user: %s select email folder: %s error: %s', self.user, send_box, e)
                continue
            results = []
            if mode == 'ON':
                today_results = self.server.search(['ON', send_date])
                before_results = self.server.search(['ON', before_send_date])
                after_results = self.server.search(['ON', after_send_date])
                results = before_results + today_results + after_results
            elif mode == 'SINCE':
                results = self.server.search(['SINCE', before_send_date])

            for mail in results:
                try:
                    email_dict = self.gen_email_dict(mail, mode, send_box, send_date)
                    if email_dict:
                        total_email_list.append(email_dict)
                except Exception as e:
                    logger.exception(e)
                    logger.error('parse email error: %s', e)
        return total_email_list

    def search_email_by_message_id(self, message_id):
        for send_box in ['INBOX', 'Sent Items']:
            logger.debug('start to search user: %s email from box: %s, message_id', self.user, send_box, message_id)
            try:
                self.server.select_folder(send_box, readonly=True)
            except Exception as e:
                logger.warning('user: %s select email folder: %s error: %s', self.user, send_box, e)
                continue
            criteria = '(HEADER Message-ID "%s")' % message_id
            try:
                results = self.server.search(criteria)
            except Exception as e:
                logger.error('search email failed: %s', e)
                return {}
            if results:
                try:
                    return self.gen_email_dict(results[0], 'SEARCH', send_box)
                except Exception as e:
                    logger.exception(e)
                    logger.error('parse email error: %s', e)
                    return {}
        return {}

    def close(self):
        self.server.logout()
