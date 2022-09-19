# simulate from https://github.com/seatable/dtable-ui-component/blob/master/src/index.js

import logging
from unittest import result

from dateutil import parser

from dtable_events.notification_rules.utils import get_nickname_by_usernames
from dtable_events.utils.constants import ARRAY_FORMAL_COLUMNS, DURATION_ZERO_DISPLAY, SIMPLE_CELL_FORMULA_RESULTS, ColumnTypes, DurationFormatsType, FormulaResultType


logger = logging.getLogger(__name__)

class BaseMessageFormatter:

    EMPTY_MESSAGE = ''

    def __init__(self, column):
        self.column = column

    def get_column_data(self):
        return self.column.get('data') or {}

    def get_options(self):
        return self.get_column_data().get('options') or []

    def format_empty_message(self):
        return self.EMPTY_MESSAGE

    def format_message(self, value):
        return ''

class CheckboxMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        return 'true' if value == True else 'false'


class ImageMessageFormatter(BaseMessageFormatter):

    EMPTY_MESSAGE = '[]'

    def format_message(self, value):
        if not isinstance(value, list) or not value:
            return self.format_empty_message()
        return '[%s]' % ', '.join([str(v) for v in value])


class LongTextMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        if isinstance(value, str):
            return value.strip()
        if not value:
            return self.format_empty_message()
        if not isinstance(value, dict):
            return self.format_empty_message()
        return str(value.get('text') or '').strip()


class TextMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        return str(value) if value else ''


class SingleSelectMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        options = self.get_options()
        for option in options:
            if option['id'] == value:
                return option['name']
        return self.format_empty_message()


class MultipleSelectFormatter(BaseMessageFormatter):

    EMPTY_MESSAGE = '[]'

    def format_message(self, value):
        if not value:
            return self.format_empty_message()
        if not isinstance(value, list):
            value = [value]
        options = self.get_options()
        real_values = []
        for v in value:
            for option in options:
                if option['id'] == v:
                    real_values.append(option['name'])
        return '[%s]' % ', '.join(real_values)


class FileMessageFormatter(BaseMessageFormatter):

    EMPTY_MESSAGE = '[]'

    def format_message(self, value):
        if not value:
            return self.format_empty_message()
        if not isinstance(value, list):
            return str(value)
        real_values = []
        for file in value:
            if not isinstance(file, dict):
                continue
            real_values.append(str(file.get('name', '')))
        return '[%s]' % ', '.join(real_values)


class LinkMessageFormatter(BaseMessageFormatter):

    EMPTY_MESSAGE = '[]'

    def format_message(self, value, db_session):
        if not value:
            return self.format_empty_message()
        if not isinstance(value, list):
            return self.format_empty_message()
        result_type = self.get_column_data().get('result_type')
        if result_type != FormulaResultType.ARRAY:
            return self.format_empty_message()
        real_values = []
        for v in value:
            if not isinstance(v, dict):
                real_values.append(str(v))
                continue
            real_values.append(str(v.get('display_value') or ''))
        return '[%s]' % ', '.join(real_values)


class CollaboratorMessageFormatter(BaseMessageFormatter):

    EMPTY_MESSAGE = '[]'

    def format_message(self, value, db_session):
        if not value:
            return self.format_empty_message()
        if not isinstance(value, list):
            value = [value]
        names_dict = get_nickname_by_usernames(value, db_session)
        names = [names_dict.get(str(user)) for user in value if user in names_dict]
        return '[%s]' % ', '.join(names)


class NumberMessageFormatter(BaseMessageFormatter):

    separator_map = {
        'comma': ',',
        'dot': '.',
        'no': '',
        'space': ' ',
    }

    currency_map = {
        'dollar': '$',
        'yuan': '￥',
        'euro': '€'
    }

    def format_message(self, value):
        if not value and value != 0:
            return self.format_empty_message()
        try:
            value = float(value)
        except:
            return self.format_empty_message()

        column_data = self.get_column_data()

        decimal = column_data.get('decimal', 'dot')
        thousands = column_data.get('thousands', 'no')
        precision = column_data.get('precision', 2)
        enable_precision = column_data.get('enable_precison', False)
        currency_symbol = column_data.get('currency_symbol', '$')
        currency_symbol_position = column_data.get('currency_symbol_position', 'before')
        number_format = column_data.get('format')  # number, percent, yuan, dollar, euro, custom_currency
        if number_format == 'percent':
            value *= 100

        value = ('%%.%sf' % precision) % value
        int_part, float_part = value.split('.')
        if thousands != 'no':
            int_part = ('{:%s}' % self.separator_map[thousands]).format(int(int_part))
        if enable_precision:
            value = int_part + self.separator_map[decimal] + float_part
        else:
            value = int_part

        if number_format in ['dollar', 'euro', 'yuan']:
            value = self.currency_map[number_format] + value
        elif number_format == 'percent':
            value += '%'
        elif number_format == 'custom_currency':
            if currency_symbol_position == 'before':
                value = currency_symbol + value
            else:
                value = value + currency_symbol

        return value


class DateMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        if not value:
            return self.format_empty_message()
        value = str(value)
        try:
            datetime_obj = parser.parse(value)
        except Exception as e:
            logger.warning('parse value: %s to datetime error: %s', value, e)
            return self.format_empty_message()
        column_data = self.get_column_data()
        format = column_data.get('format')
        if format == 'D/M/YYYY':
            value = datetime_obj.strftime('%-d/%-m/%Y')
        elif format == 'DD/MM/YYYY':
            value = datetime_obj.strftime('%d/%m/%Y')
        elif format == 'D/M/YYYY HH:mm':
            value = datetime_obj.strftime('%-d/%-m/%Y %H:%M')
        elif format == 'DD/MM/YYYY HH:mm':
            value = datetime_obj.strftime('%d/%m/%Y %H:%M')
        elif format == 'M/D/YYYY':
            value = datetime_obj.strftime('%-m/%-d/%Y')
        elif format == 'M/D/YYYY HH:mm':
            value = datetime_obj.strftime('%-m/%-d/%Y %H:%M')
        elif format == 'YYYY-MM-DD':
            value = datetime_obj.strftime('%Y-%m-%d')
        elif format == 'YYYY-MM-DD HH:mm':
            value = datetime_obj.strftime('%Y-%m-%d %H:%M')
        elif format == 'DD.MM.YY':
            value = datetime_obj.strftime('%d.%m.%Y')
        elif format == 'DD.MM.YY HH:mm':
            value = datetime_obj.strftime('%d.%m.%Y %H:%M')
        else:
            value = datetime_obj.strftime('%Y-%m-%d')
        return value


class CreatorMessageFormatter(BaseMessageFormatter):

    def format_message(self, value, db_session):
        if not value:
            return self.format_empty_message()
        value = str(value)
        user_dict = get_nickname_by_usernames([value], db_session)
        return user_dict.get(value, value)


class CTimeMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        if not value:
            return self.format_empty_message()
        value = str(value)
        try:
            datetime_obj = parser.parse(value)
        except Exception as e:
            logger.warning('parse value: %s to datetime error: %s', value, e)
            return self.format_empty_message()
        value = datetime_obj.strftime('%Y-%m-%d %H:%M')
        return value


class LastModifierMessageFormatter(CreatorMessageFormatter):
    pass


class MTimeMessageFormatter(CTimeMessageFormatter):
    pass


class GeolocationMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        if not value:
            return self.format_empty_message()
        if not isinstance(value, dict):
            return self.format_empty_message()
        info_list = []
        province = value.get('province', '')
        city = value.get('city', '')
        district = value.get('district', '')
        detail = value.get('detail', '')
        country_region = value.get('country_region', '')

        lng = value.get('lng', '')
        lat = value.get('lat', '')

        if country_region:
            info_list.append(country_region)
        if province:
            info_list.append(province)
        if city:
            info_list.append(city)
        if district:
            info_list.append(district)
        if detail:
            info_list.append(detail)

        if lng:
            info_list.append("lng: %s" % lng)
        if lat:
            info_list.append("lat: %s" % lat)

        return info_list and " ".join(info_list) or ''


def flat(value):
    if not isinstance(value, list):
        return []
    real_value = []
    for v in value:
        if isinstance(v, list):
            real_value.extend(v)
        else:
            real_value.append(v)
    return real_value


class FormulaMessageFormatter(BaseMessageFormatter):

    def format_message(self, value, db_session):
        if not value and value != 0 and value != False:
            return self.format_empty_message()
        column_data = self.get_column_data()
        result_type = column_data.get('result_type')
        array_type = column_data.get('array_type')
        array_data = column_data.get('array_data')

        if result_type == FormulaResultType.STRING:
            return TextMessageFormatter({'data': column_data}).format_message(value)
        elif result_type == FormulaResultType.NUMBER:
            return NumberMessageFormatter({'data': column_data}).format_message(value)
        elif result_type == FormulaResultType.DATE:
            return DateMessageFormatter({'data': column_data}).format_message(value)
        elif result_type == FormulaResultType.BOOL:
            return CheckboxMessageFormatter({'data': column_data}).format_message(value)
        elif result_type == FormulaResultType.ARRAY:
            if not isinstance(value, list):
                return '[]'
            if array_type in [
                ColumnTypes.LINK,
                ColumnTypes.LINK_FORMULA,
                ColumnTypes.FORMULA
            ]:
                return '[]'
            if array_type in ARRAY_FORMAL_COLUMNS:
                formatter_class = formatter_map.get(array_type)
                if not formatter_class:
                    return '[]'
                formatter_params = create_formatter_params(array_type, value=list(set(flat(value))), db_session=db_session)
                return formatter_class({'data': array_data}).format_message(**formatter_params)
            if array_type == FormulaResultType.STRING:
                array_type = ColumnTypes.TEXT
            elif array_type == FormulaResultType.BOOL:
                array_type = ColumnTypes.CHECKBOX
            formatter_class = formatter_map.get(array_type)
            if not formatter_class:
                    return '[]'
            real_values = []
            for v in value:
                formatter_params = create_formatter_params(array_type, value=v, db_session=db_session)
                tmp = formatter_class({'data': array_data}).format_message(**formatter_params)
                if not tmp:
                    continue
                real_values.append(tmp)
            return '[%s]' % ', '.join(real_values)
        else:
            return '[]'


class LinkFormulaMessageFormatter(FormulaMessageFormatter):
    pass


class AutoNumberMessageFormatter(TextMessageFormatter):
    pass


class URLMessageFormatter(TextMessageFormatter):
    pass


class EmailMessageFormatter(TextMessageFormatter):
    pass


class DurationMessageFormatter(BaseMessageFormatter):

    def format_message(self, value):
        if not value and value != 0:
            return self.format_empty_message()
        try:
            value = int(float(value))
        except:
            return self.format_empty_message()
        duration_format = self.get_column_data().get('duration_format') or DurationFormatsType.H_MM
        if value == 0:
            return DURATION_ZERO_DISPLAY[duration_format]
        prefix = '' if value >= 0 else '-'
        hours = abs(value) // 3600
        minutes = (abs(value) % 3600) // 60
        seconds = abs(value) % 60
        if duration_format == 'h:mm':
            value = '%s%d:%2d' % (prefix, hours, minutes)
        elif duration_format == 'h:mm:ss':
            value = '%s%d:%02d:%02d' % (prefix, hours, minutes, seconds)
        else:
            value = '%s%d:%2d' % (prefix, hours, minutes)
        return value


class RateMessageFormatter(TextMessageFormatter):
    pass


formatter_map = {
    ColumnTypes.CHECKBOX: CheckboxMessageFormatter,
    ColumnTypes.IMAGE: ImageMessageFormatter,
    ColumnTypes.LONG_TEXT: LongTextMessageFormatter,
    ColumnTypes.TEXT: TextMessageFormatter,
    ColumnTypes.SINGLE_SELECT: SingleSelectMessageFormatter,
    ColumnTypes.MULTIPLE_SELECT: MultipleSelectFormatter,
    ColumnTypes.FILE: FileMessageFormatter,
    ColumnTypes.LINK: LinkMessageFormatter,
    ColumnTypes.COLLABORATOR: CollaboratorMessageFormatter,
    ColumnTypes.NUMBER: NumberMessageFormatter,
    ColumnTypes.DATE: DateMessageFormatter,
    ColumnTypes.CREATOR: CreatorMessageFormatter,
    ColumnTypes.CTIME: CTimeMessageFormatter,
    ColumnTypes.LAST_MODIFIER: LastModifierMessageFormatter,
    ColumnTypes.MTIME: MTimeMessageFormatter,
    ColumnTypes.GEOLOCATION: GeolocationMessageFormatter,
    ColumnTypes.FORMULA: FormulaMessageFormatter,
    ColumnTypes.AUTO_NUMBER: AutoNumberMessageFormatter,
    ColumnTypes.URL: URLMessageFormatter,
    ColumnTypes.EMAIL: EmailMessageFormatter,
    ColumnTypes.DURATION: DurationMessageFormatter,
    ColumnTypes.RATE: RateMessageFormatter,
    ColumnTypes.LINK_FORMULA: LinkFormulaMessageFormatter
}


def create_formatter_params(formatter_type, **kwargs):
    if formatter_type not in formatter_map:
        return {}
    value = kwargs.get('value')
    db_session = kwargs.get('db_session')
    params = {'value': value}
    if formatter_type in [
        ColumnTypes.COLLABORATOR,
        ColumnTypes.LINK,
        ColumnTypes.LINK_FORMULA,
        ColumnTypes.CREATOR,
        ColumnTypes.LAST_MODIFIER,
        ColumnTypes.FORMULA
    ]:
        params['db_session'] = db_session
    return params


def number_validator(value):
    return (value or value == 0) and type(value) in (int, float)


def text_validator(value):
    return value


def checkbox_validator(value):
    return isinstance(value, bool)


validators = {
    ColumnTypes.NUMBER: number_validator,
    ColumnTypes.RATE: number_validator,
    ColumnTypes.DURATION: number_validator,
    ColumnTypes.CHECKBOX: checkbox_validator,
    ColumnTypes.TEXT: text_validator,
    ColumnTypes.DATE: text_validator,
    ColumnTypes.CTIME: text_validator,
    ColumnTypes.MTIME: text_validator,
    ColumnTypes.GEOLOCATION: text_validator,
    ColumnTypes.AUTO_NUMBER: text_validator,
    ColumnTypes.URL: text_validator,
    ColumnTypes.EMAIL: text_validator,
    FormulaResultType.DATE: text_validator,
    FormulaResultType.STRING: text_validator,
    FormulaResultType.NUMBER: number_validator,
    FormulaResultType.BOOL: checkbox_validator
}


def value_validator(value, column_type):
    if column_type in validators:
        return validators[column_type](value)
    return True
