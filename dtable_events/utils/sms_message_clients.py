import logging
import json
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest

logger = logging.getLogger(__name__)


class AliyunSmsMessageClient:
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '__instance__'):
            setattr(cls, '__instance__', super().__new__(cls))
        return getattr(cls, '__instance__')

    def __init__(self):
        if not hasattr(self, '_client'):
            from seahub.settings import ALIYUN_SMS_MESSAGE_CONFIG
            self._client = AcsClient(
                ak=ALIYUN_SMS_MESSAGE_CONFIG['accessKeyId'],
                secret=ALIYUN_SMS_MESSAGE_CONFIG['accessKeySecret'],
                region_id=ALIYUN_SMS_MESSAGE_CONFIG.get('regionId'))

    def send_message(self, phone, template_name, msg_dict):
        # https://help.aliyun.com/document_detail/419273.html
        from seahub.settings import ALIYUN_SMS_MESSAGE_CONFIG
        request = CommonRequest()
        request.set_accept_format('json')
        request.set_domain('dysmsapi.aliyuncs.com')
        request.set_method('POST')
        request.set_protocol_type('https')
        request.set_action_name('SendSms')
        request.set_version('2017-05-25')

        templates = ALIYUN_SMS_MESSAGE_CONFIG['templates']
        target_template = None
        for template in templates:
            if template['name'] == template_name:
                target_template = template
                break

        request.add_query_param('SignName', target_template['signName'])
        request.add_query_param('TemplateCode', target_template['templateCode'])
        request.add_query_param('RegionId', target_template.get('regionId'))
        request.add_query_param('PhoneNumbers', phone)
        request.add_query_param('TemplateParam', json.dumps(msg_dict))
        self._client.do_action_with_exception(request)
        logger.debug('has send to: %s template_name: %s', phone, template_name)
