from datetime import datetime

from dtable_events.utils.constants import ColumnTypes, FORMULA_COLUMN_TYPES, FormulaResultType, NUMERIC_COLUMNS_TYPES, \
    DATE_COLUMN_TYPES

def get_check_type(column):
    if not column:
        return ''
    type = column.get('type', '')
    data = column.get('data', {}) or {}
    
    if type in FORMULA_COLUMN_TYPES:
        result_type = data.get('result_type', '')
        array_type = data.get('array_type', '')
        if result_type == FormulaResultType.ARRAY:
            return array_type
        return result_type

    if type == ColumnTypes.LINK:
      array_type = data.get('array_type', '')
      return array_type
    return type

def is_numeric_column(column):
    check_type = get_check_type(column)
    return check_type in NUMERIC_COLUMNS_TYPES

def is_date_column(column):
    check_type = get_check_type(column)
    return check_type in DATE_COLUMN_TYPES


class AutoNumberUtils:  
    AUTO_NUMBER_PREFIX_TYPE = {  
        "STRING": "string",
        "DATE": "date"
    }
    AUTO_NUMBER_DATE_FORMAT = "%Y%m%d"

    @classmethod
    def get_parsed_format(cls, format):
        if not format:
            return {"digits": 1, "prefix_type": None, "prefix": None}
        format_items = format.split('-')
        if len(format_items) not in [1, 2]:
            return {}
        digit_string = None
        prefix_type = None
        prefix = None
        if len(format_items) == 1:
            digit_string = format_items[0]
        else:
            prefix = format_items[0]
            digit_string = format_items[1]
            if prefix == cls.AUTO_NUMBER_DATE_FORMAT:
                prefix = datetime.now().strftime(cls.AUTO_NUMBER_DATE_FORMAT)
                prefix_type = cls.AUTO_NUMBER_PREFIX_TYPE["DATE"]
            else:  
                prefix_type = cls.AUTO_NUMBER_PREFIX_TYPE["STRING"]
        if not cls.is_valid_digits(digit_string):
            return {}
        return {"digits": len(digit_string), "prefix_type": prefix_type, "prefix": prefix}

    @classmethod
    def is_valid_digits(cls, digit_string):
        if len(digit_string) == 0:
            return False
        return digit_string == ''.join(str(0) for _ in range(len(digit_string)))
