import logging

from dateutil import parser

from dtable_events.utils.constants import ColumnTypes

logger = logging.getLogger(__name__)


class BaseValidator:

    def __init__(self, column):
        self.column = column


class TextValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, str):
            return None
        return value


class NumberValidator(BaseValidator):
    def validate(self, value):
        if value is None:
            return None
        try:
            return float(value)
        except:
            return None


class LongTextValidator(BaseValidator):

    def validate(self, value):
        if value is None:
            return None
        if isinstance(value, dict):
            return value.get('text')
        elif isinstance(value, str):
            return value
        return None


class ImageValidator(BaseValidator):

    def validate(self, value):
        if value is None:
            return None
        if isinstance(value, list):
            if value and not isinstance(value[0], str):
                return None
            return [v for v in value if isinstance(v, str)]
        return None


class DateValidator(BaseValidator):

    def validate(self, value):
        if value is None:
            return value
        if not isinstance(value, str):
            return None
        try:
            parser.parse(value)
        except:
            return None
        return value


class CheckboxValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, bool):
            return None
        return value


class SingleSelectValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, str):
            return None
        column_data = self.column.get('data') or {}
        options = column_data.get('options') or []
        for option in options:
            if option['id'] == value:
                return option['name']
        return None


class MultipleSelectValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, list) or (value and not isinstance(value[0], str)):
            return None
        column_data = self.column.get('data') or {}
        options = column_data.get('options') or []
        options_dict = {option['id']: option for option in options}
        return [options_dict[v]['name'] for v in value] or None


class URLValiadator(TextValidator):
    pass


class DurationValidator(BaseValidator):

    def validate(self, value):
        try:
            return int(value)
        except:
            return None


class FileValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, list) or (value and not isinstance(value[0], dict)):
            return None
        real_values = []
        for v in value:
            if not isinstance(v, dict):
                continue
            # TODO: check url name ...
            real_values.append(v)
        return real_values


class CollaboratorValidator(BaseValidator):

    def validate(self, value):
        if not isinstance(value, list) or (value and not isinstance(value[0], str)):
            return None
        return [v for v in value if isinstance(v, str)]


class EmailValidator(TextValidator):
    pass


class GeolocationValidator(BaseValidator):

    def is_valid_position(self, lng, lat):
        return (lng or lng == 0) and (lat or lat == 0)

    def validate(self, value):
        if not isinstance(value, dict):
            return None
        column_data = self.column.get('data') or {}
        geo_format = column_data.get('geo_format')
        if geo_format == 'lng_lat':
            lng, lat = value.get('lng'), value.get('lat')
            if not (lng and lat):
                return None
            if not self.is_valid_position(lng, lat):
                return None
            return value
        elif geo_format == 'country_region':
            return value.get('country_region', '')
        elif geo_format == 'geolocation':
            province, city, district, detail = value.get('province', ''), value.get('city', ''), value.get('district', ''), value.get('detail', '')
            return {
                'province': province,
                'city': city,
                'district': district,
                'detail': detail
            }
        elif geo_format == 'province_city_district':
            province, city, district = value.get('province', ''), value.get('city', ''), value.get('district', '')
            return {
                'province': province,
                'city': city,
                'district': district
            }
        elif geo_format == 'province':
            province = value.get('province', '')
            return {
                'province': province
            }
        elif geo_format == 'province_city':
            province, city = value.get('province', ''), value.get('city', '')
            return {
                'province': province,
                'city': city
            }
        else:
            return value


class RateValidator(BaseValidator):

    def validate(self, value):
        try:
            return int(value)
        except:
            return None


class DurationValidator(BaseValidator):

    def validate(self, value):
        try:
            return int(value)
        except:
            return None


class EmailValidator(TextValidator):
    pass


class CreatorValidator(TextValidator):
    pass


class CTimeValidator(DateValidator):
    pass


class LastModifierValidator(TextValidator):
    pass


class MTimeModifierValidator(DateValidator):
    pass


VALIDATORS_MAP = {
    ColumnTypes.TEXT: TextValidator,
    ColumnTypes.NUMBER: NumberValidator,
    ColumnTypes.LONG_TEXT: LongTextValidator,
    ColumnTypes.IMAGE: ImageValidator,
    ColumnTypes.DATE: DateValidator,
    ColumnTypes.CHECKBOX: CheckboxValidator,
    ColumnTypes.SINGLE_SELECT: SingleSelectValidator,
    ColumnTypes.MULTIPLE_SELECT: MultipleSelectValidator,
    ColumnTypes.URL: URLValiadator,
    ColumnTypes.FILE: FileValidator,
    ColumnTypes.COLLABORATOR: CollaboratorValidator,
    ColumnTypes.GEOLOCATION: GeolocationValidator,
    ColumnTypes.RATE: RateValidator,
    ColumnTypes.DURATION: DurationValidator,
    ColumnTypes.EMAIL: EmailValidator,
    ColumnTypes.CREATOR: CreatorValidator,
    ColumnTypes.CTIME: CTimeValidator,
    ColumnTypes.LAST_MODIFIER: LastModifierValidator,
    ColumnTypes.MTIME: MTimeModifierValidator
}


def validate_table_db_cell_value(column, value):
    if column['type'] not in VALIDATORS_MAP:
        return None
    try:
        return VALIDATORS_MAP[column['type']](column).validate(value)
    except Exception as e:
        logger.exception(e)
        logger.error('validate column : %s, value: %s error: %s', column, value, e)
    return None
