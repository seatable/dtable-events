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
    check_type = get_check_type(column);
    return check_type in NUMERIC_COLUMNS_TYPES

def is_date_column(column):
    check_type = get_check_type(column);
    return check_type in DATE_COLUMN_TYPES

