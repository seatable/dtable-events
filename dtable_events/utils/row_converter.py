import logging
import re
from datetime import datetime

from dateutil import parser

from dtable_events.utils.constants import FormulaResultType, ColumnTypes
from dtable_events.notification_rules.notification_rules_utils import fill_cell_with_sql_row
from dtable_events.utils.message_formatters import FormulaMessageFormatter

logger = logging.getLogger(__name__)


class BaseCellConverter:

    def __init__(self, column):
        self.column = column

    def get_column_data(self):
        return self.column.get('data') or {}

    def convert(self, value):
        return value


class SingleSelectCellConverter(BaseCellConverter):

    def convert(self, value):
        column_data = self.get_column_data()
        options = column_data.get('options') or []
        option = next(filter(lambda option: option.get('id') == value, options), None)
        return option.get('name') or None


class MultiSelectCellConverter(BaseCellConverter):

    def convert(self, value):
        column_data = self.get_column_data()
        options = column_data.get('options') or []
        result = []
        if not isinstance(value, list):
            return ''
        for item in value:
            item_option = next(filter(lambda option: option.get('id') == item, options), None)
            if item_option:
                result.append(item_option['name'])
        return result


class LongTextCellConverter(BaseCellConverter):

    def convert(self, value):
        if isinstance(value, str):
            return value
        elif isinstance(value, dict):
            return value.get('text') or ''
        return ''


class DateCellConverter(BaseCellConverter):

    def format_text_to_date(self, text, format='YYYY-MM-DD'):
        if not isinstance(text, str):
            return None
        is_all_number = re.match(r'^\d+$', text)
        if is_all_number:
            date_obj = datetime.fromtimestamp(int(text) / 1000)
        else:
            date_obj = parser.parse(text)
        if 'HH:mm' not in format:
            return datetime.strftime(date_obj, '%Y-%m-%d')
        return datetime.strftime(date_obj, '%Y-%m-%d %H:%M')

    def convert(self, value):
        value = re.sub(r'([+-]\d{2}:?\d{2})|Z$', '', value)
        column_data = self.get_column_data()
        format = column_data.get('format') or 'YYYY-MM-DD'
        return self.format_text_to_date(value, format)


class FormulaCellConverter(BaseCellConverter):

    def convert(self, value, db_session):
        column_data = self.get_column_data()
        if not column_data:
            return None
        result_type = column_data.get('result_type')
        if result_type in [
            FormulaResultType.NUMBER,
            FormulaResultType.DATE,
        ]:
            return FormulaMessageFormatter(self.column).format_message(value, None)
        if isinstance(value, bool):
            return 'true' if bool else 'false'
        if result_type == FormulaResultType.ARRAY:
            array_type = column_data.get('array_type')
            array_data = column_data.get('array_data')
            if not array_type:
                return ''
            if array_type in [
                ColumnTypes.COLLABORATOR,
                ColumnTypes.CREATOR,
                ColumnTypes.LAST_MODIFIER
            ]:
                return value
            if array_type not in [
                ColumnTypes.IMAGE,
                ColumnTypes.FILE,
                ColumnTypes.MULTIPLE_SELECT,
                ColumnTypes.COLLABORATOR
            ] and isinstance(value, list):
                return FormulaMessageFormatter(self.column).format_message(value, db_session)
            return fill_cell_with_sql_row(value, {'data': array_data}, db_session)
        return value


def convert_row_with_sql_row(row, columns, db_session):
    result = {}
    if '_id' in row:
        result['_id'] = row['_id']
    if '_ctime' in row:
        result['_ctime'] = row['_ctime']
    if '_mtime' in row:
        result['_mtime'] = row['_mtime']
    for column in columns:
        value = row.get(column['key'])
        if not value:
            continue
        column_name = column['name']
        if not column:
            continue
        try:
            if column['type'] == ColumnTypes.SINGLE_SELECT:
                result[column_name] = SingleSelectCellConverter(column).convert(value)
            elif column['type'] == ColumnTypes.MULTIPLE_SELECT:
                result[column_name] = MultiSelectCellConverter(column).convert(value)
            elif column['type'] == ColumnTypes.DATE:
                result[column_name] = DateCellConverter(column).convert(value)
            elif column['type'] == ColumnTypes.LONG_TEXT:
                result[column_name] = LongTextCellConverter(column).convert(value)
            elif column['type'] in [
                ColumnTypes.FORMULA,
                ColumnTypes.LINK_FORMULA
            ]:
                result[column_name] = FormulaCellConverter(column).convert(value, db_session)
            else:
                result[column_name] = BaseCellConverter(column).convert(value)
        except Exception as e:
            logger.warning('convert row column %s with value %s', column, value)
            result[column_name] = value

    return result
