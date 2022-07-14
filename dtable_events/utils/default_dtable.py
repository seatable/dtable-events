import uuid
import base64
import json
from datetime import datetime

try:
    from seahub.settings import DEFAULT_DTABLE_FORMAT_VERSION
except ImportError as err:
    DEFAULT_DTABLE_FORMAT_VERSION = 9

'''
dtable-server/src/model/default-dtable.js generatorDefaultData
dtable-store/src/operations/apply.js UPDATE_DEFAULT_DATA
'''


class DefaultDtable(object):

    def __init__(self, username, user_language):
        self.username = username
        self.user_language = user_language

    def generate(self):
        self.get_user_language()
        self.data = {
            'version': 1,
            'format_version': DEFAULT_DTABLE_FORMAT_VERSION,
            'statistics': [],
            'links': [],
            'tables': [{
                '_id': '0000',
                'name': 'Table1',
                'rows': [
                    self.gen_new_row(),
                    self.gen_new_row(),
                    self.gen_new_row(),
                ],
                'columns': [{
                    'key': '0000',
                    'name': self.get_column_default_name(),
                    'type': 'text',
                    'width': 200,
                    'editable': True,
                    'resizable': True,
                }],
                'view_structure': {
                    'folders': [],
                    'view_ids': ['0000'],
                },
                'views': [{
                    '_id': '0000',
                    'name': self.get_view_default_name(),
                    'type': 'table',
                    'is_locked': False,
                    'rows': [],
                    'formula_rows': {},
                    'summaries': {},
                    'filter_conjunction': 'And',
                    'filters': [],
                    'sorts': [],
                    'hidden_columns': [],
                    'groupbys': [],
                    'groups': [],
                    'colors': {},
                    'column_colors': {},
                    'link_rows': {},
                }],
                'id_row_map': {},
            }],
        }
        self.json_data = json.dumps(self.data)
        return self.json_data

    def gen_new_row(self):
        now = self.get_now()
        new_row = {
            '_id': self.gen_row_id(),
            '_participants': [],
            '_creator': self.username,
            '_ctime': now,
            '_last_modifier': self.username,
            '_mtime': now,
        }
        return new_row

    def gen_row_id(self):
        ''' slugid.nice()
        '''
        rawBytes = bytearray(uuid.uuid4().bytes)
        rawBytes[0] = rawBytes[0] & 0x7f  # Ensure slug starts with [A-Za-f]
        slug = base64.urlsafe_b64encode(rawBytes)[:-2]
        if isinstance(slug, bytes):
            slug = slug.decode('utf-8')
        return slug

    def get_now(self):
        now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + '+00:00'
        return now

    def get_user_language(self):
        return self.user_language

    def get_view_default_name(self):
        if self.user_language == 'zh-cn':
            return '默认视图'
        else:
            return 'Default View'

    def get_column_default_name(self):
        if self.user_language == 'zh-cn':
            return '名称'
        else:
            return 'Name'
