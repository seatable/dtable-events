import re
from datetime import timedelta
from functools import cmp_to_key

from dateutil import parser as date_parser

from dtable_events.utils.constants import (
    ColumnTypes,
    FormulaResultType,
    NUMERIC_COLUMNS_TYPES,
    DATE_COLUMN_TYPES,
    FORMULA_COLUMN_TYPES,
)

MAX_GROUP_LEVEL = 3

WEEK_DAY_TO_NUM_MAP = {'Monday': 1, 'Saturday': 6, 'Sunday': 0}
MONTH_QUARTERS = [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]

TEXT_SORTER_COLUMN_TYPES = [ColumnTypes.TEXT, ColumnTypes.URL, ColumnTypes.EMAIL]
NUMBER_SORTER_COLUMN_TYPES = [ColumnTypes.NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE]
MULTIPLE_CELL_VALUE_COLUMN_TYPES = [ColumnTypes.MULTIPLE_SELECT, ColumnTypes.COLLABORATOR, ColumnTypes.LINK]


def _get_column_type(column):
    column_type = column.get('type')
    data = column.get('data') or {}
    if column_type in FORMULA_COLUMN_TYPES:
        result_type = data.get('result_type')
        if result_type == FormulaResultType.ARRAY:
            return data.get('array_type')
        return result_type
    if column_type == ColumnTypes.LINK:
        return data.get('array_type')
    return column_type


def _is_numeric_column(column):
    return _get_column_type(column) in NUMERIC_COLUMNS_TYPES


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_empty(value):
    if value is None:
        return True
    if isinstance(value, str) and value == '':
        return True
    if isinstance(value, (list, tuple, dict)) and len(value) == 0:
        return True
    return False


def _make_hashable(value):
    if isinstance(value, list):
        return tuple(_make_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((key, _make_hashable(item)) for key, item in value.items()))
    return value


def _get_date_granularity_value(cell_value, count_type, first_day_of_week=None):
    if not cell_value:
        return ''
    try:
        dt = date_parser.parse(str(cell_value))
    except (ValueError, TypeError, OverflowError):
        return ''
    granularity = (count_type or '').lower()
    if granularity == 'year':
        return str(dt.year)
    if granularity == 'quartar':
        quarter = MONTH_QUARTERS[dt.month - 1]
        return '%s-Q%s' % (dt.year, quarter)
    if granularity == 'month':
        return '%s-%02d' % (dt.year, dt.month)
    if granularity == 'week':
        first_day_num = WEEK_DAY_TO_NUM_MAP.get(first_day_of_week, 0)
        js_weekday = (dt.weekday() + 1) % 7
        days_to_first = (js_weekday - first_day_num + 7) % 7
        week_start = dt - timedelta(days=days_to_first)
        return week_start.strftime('%Y-%m-%d')
    return dt.strftime('%Y-%m-%d')


def _get_geolocation_granularity_value(cell_value, count_type):
    if not isinstance(cell_value, dict):
        return ''
    granularity = (count_type or '').lower()
    if granularity == 'city':
        return cell_value.get('city') or ''
    if granularity == 'district':
        return cell_value.get('district') or ''
    if granularity == 'country':
        return cell_value.get('country_region') or ''
    return cell_value.get('province') or ''


def _get_option_name_index_map(column):
    data = column.get('data') or {}
    options = data.get('options') or []
    return {option.get('name'): index for index, option in enumerate(options) if option.get('name') is not None}


def _normalize_formula_group_value(cell_value, column, count_type, email2nickname=None, first_day_of_week=None):
    data = column.get('data') or {}
    result_type = data.get('result_type')
    if result_type == FormulaResultType.ARRAY:
        return cell_value, cell_value, _make_hashable(cell_value)
    if result_type in DATE_COLUMN_TYPES:
        value = _get_date_granularity_value(cell_value, count_type, first_day_of_week)
        return value, value, value
    if result_type == FormulaResultType.BOOL:
        value = bool(cell_value)
        return value, value, value
    return cell_value, cell_value, _make_hashable(cell_value)


def _normalize_group_cell_value(row, column, group_by, email2nickname=None, first_day_of_week=None):
    column_type = column.get('type')
    cell_value = row.get(column.get('name'))
    count_type = group_by.get('count_type')

    if column_type in FORMULA_COLUMN_TYPES:
        return _normalize_formula_group_value(cell_value, column, count_type, email2nickname, first_day_of_week)

    if column_type == ColumnTypes.LINK:
        items = cell_value if isinstance(cell_value, list) else ([cell_value] if cell_value else [])
        display_values = [item.get('display_value') if isinstance(item, dict) else item for item in items]
        return display_values, display_values, tuple(sorted(str(value) for value in display_values))

    if column_type in DATE_COLUMN_TYPES:
        value = _get_date_granularity_value(cell_value, count_type, first_day_of_week)
        return value, value, value

    if column_type == ColumnTypes.GEOLOCATION:
        value = _get_geolocation_granularity_value(cell_value, count_type)
        return value, value, value

    if column_type == ColumnTypes.SINGLE_SELECT:
        return cell_value, cell_value, cell_value

    if column_type == ColumnTypes.MULTIPLE_SELECT:
        names = cell_value if isinstance(cell_value, list) else []
        return names, ' '.join(names), tuple(sorted(names))

    if column_type in (ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER):
        emails = cell_value if isinstance(cell_value, list) else ([cell_value] if cell_value else [])
        names = [email2nickname.get(email, email) for email in emails] if email2nickname else emails
        return names, ' '.join(names), tuple(sorted(names))

    if column_type == ColumnTypes.CHECKBOX:
        value = bool(cell_value)
        return value, value, value

    return cell_value, cell_value, _make_hashable(cell_value)


def _compare_string(left, right):
    if not left and not right:
        return 0
    if not left:
        return -1
    if not right:
        return 1
    if not isinstance(left, str) or not isinstance(right, str):
        return 0
    left_parts = re.findall(r'\d+|\D+', left)
    right_parts = re.findall(r'\d+|\D+', right)
    length = min(len(left_parts), len(right_parts))
    for index in range(length):
        left_part = left_parts[index]
        right_part = right_parts[index]
        if left_part.isdigit() and right_part.isdigit():
            left_number = int(left_part)
            right_number = int(right_part)
            if left_number > right_number:
                return 1
            if left_number < right_number:
                return -1
        if left_part != right_part:
            return -1 if left < right else 1
    return -1 if left < right else (1 if left > right else 0)


def _sort_text(left, right, sort_type):
    empty_left = not left
    empty_right = not right
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    if left == right:
        return 0
    result = _compare_string(left, right)
    return result if sort_type == 'up' else -result


def _sort_number(left, right, sort_type):
    empty_left = not _is_number(left)
    empty_right = not _is_number(right)
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    if left > right:
        return 1 if sort_type == 'up' else -1
    if left < right:
        return -1 if sort_type == 'up' else 1
    return 0


def _sort_date(left, right, sort_type):
    empty_left = not left
    empty_right = not right
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    if left > right:
        return 1 if sort_type == 'up' else -1
    if left < right:
        return -1 if sort_type == 'up' else 1
    return 0


def _sort_checkbox(left, right, sort_type):
    left_checked = 1 if left else -1
    right_checked = 1 if right else -1
    if left_checked > right_checked:
        return 1 if sort_type == 'up' else -1
    if left_checked < right_checked:
        return -1 if sort_type == 'up' else 1
    return 0


def _sort_single_select(left, right, sort_type, option_index_map):
    left_index = option_index_map.get(left)
    right_index = option_index_map.get(right)
    empty_left = left_index is None
    empty_right = right_index is None
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    if left_index > right_index:
        return 1 if sort_type == 'up' else -1
    if left_index < right_index:
        return -1 if sort_type == 'up' else 1
    return 0


def _get_multiple_indexes_orderby_options(option_names, option_index_map):
    indexes = [option_index_map[name] for name in option_names if option_index_map.get(name) is not None]
    return sorted(indexes)


def _sort_multiple_select(left, right, sort_type, option_index_map):
    empty_left = not left or len(left) == 0
    empty_right = not right or len(right) == 0
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    left_indexes = _get_multiple_indexes_orderby_options(left, option_index_map)
    right_indexes = _get_multiple_indexes_orderby_options(right, option_index_map)
    if len(left_indexes) == len(right_indexes) and (len(left_indexes) == 0 or left_indexes == right_indexes):
        return 0
    length = min(len(left_indexes), len(right_indexes))
    for index in range(length):
        if left_indexes[index] > right_indexes[index]:
            return 1 if sort_type == 'up' else -1
        if left_indexes[index] < right_indexes[index]:
            return -1 if sort_type == 'up' else 1
    if len(left_indexes) > len(right_indexes):
        return 1 if sort_type == 'up' else -1
    return -1 if sort_type == 'up' else 1


def _sort_collaborator(left, right, sort_type):
    left_string = ''.join(left) if isinstance(left, list) and len(left) else None
    right_string = ''.join(right) if isinstance(right, list) and len(right) else None
    return _sort_text(left_string, right_string, sort_type)


def _sort_department(left, right, sort_type):
    empty_left = left is None or left == ''
    empty_right = right is None or right == ''
    if empty_left and empty_right:
        return 0
    if empty_left:
        return 1
    if empty_right:
        return -1
    if left > right:
        return 1 if sort_type == 'down' else -1
    if left < right:
        return -1 if sort_type == 'down' else 1
    return 0


def _formula_display_string(cell_value, column_data):
    if isinstance(cell_value, list):
        return ' '.join(str(item) for item in cell_value)
    if cell_value is None:
        return None
    return str(cell_value)


def _sort_by_array_type(left, right, sort_type, column_data, email2nickname=None):
    array_type = (column_data or {}).get('array_type')
    if array_type in NUMBER_SORTER_COLUMN_TYPES:
        left_number = left[0] if isinstance(left, list) else left
        right_number = right[0] if isinstance(right, list) else right
        return _sort_number(left_number, right_number, sort_type)
    if array_type in DATE_COLUMN_TYPES:
        left_date = left[0] if isinstance(left, list) else left
        right_date = right[0] if isinstance(right, list) else right
        return _sort_date(left_date, right_date, sort_type)
    if array_type in (ColumnTypes.CHECKBOX, FormulaResultType.BOOL):
        left_bool = (left[0] if isinstance(left, list) else left) or False
        right_bool = (right[0] if isinstance(right, list) else right) or False
        return _sort_checkbox(left_bool, right_bool, sort_type)
    if array_type == ColumnTypes.COLLABORATOR:
        left_collaborators = left if isinstance(left, list) else [left]
        right_collaborators = right if isinstance(right, list) else [right]
        return _sort_collaborator(left_collaborators, right_collaborators, sort_type)
    return _sort_text(
        _formula_display_string(left, column_data),
        _formula_display_string(right, column_data),
        sort_type,
    )


def _sort_link(left, right, sort_type, column_data, email2nickname=None):
    if _is_empty(left) and _is_empty(right):
        return 0
    if _is_empty(left):
        return 1
    if _is_empty(right):
        return -1
    return _sort_by_array_type(left, right, sort_type, column_data, email2nickname)


def _sort_formula(left, right, sort_type, column_data, email2nickname=None):
    result_type = (column_data or {}).get('result_type')
    if result_type in NUMBER_SORTER_COLUMN_TYPES:
        return _sort_number(left, right, sort_type)
    if result_type in DATE_COLUMN_TYPES:
        return _sort_date(left, right, sort_type)
    if result_type == FormulaResultType.BOOL:
        return _sort_checkbox(left or False, right or False, sort_type)
    if result_type == FormulaResultType.ARRAY:
        return _sort_by_array_type(left, right, sort_type, column_data, email2nickname)
    return _sort_text(
        _formula_display_string(left, column_data),
        _formula_display_string(right, column_data),
        sort_type,
    )


def _compare_group_values(column, left, right, sort_type, option_index_map, email2nickname=None):
    column_type = column.get('type')
    effective_type = _get_column_type(column)
    column_data = column.get('data') or {}

    if column_type == ColumnTypes.LINK:
        return _sort_link(left, right, sort_type, column_data, email2nickname)
    if column_type in FORMULA_COLUMN_TYPES:
        return _sort_formula(left, right, sort_type, column_data, email2nickname)

    if effective_type in TEXT_SORTER_COLUMN_TYPES or effective_type == FormulaResultType.STRING:
        return _sort_text(left, right, sort_type)
    if effective_type in NUMBER_SORTER_COLUMN_TYPES:
        return _sort_number(left, right, sort_type)
    if effective_type in DATE_COLUMN_TYPES:
        return _sort_date(left, right, sort_type)
    if effective_type in (ColumnTypes.CHECKBOX, FormulaResultType.BOOL):
        return _sort_checkbox(left or False, right or False, sort_type)
    if effective_type == ColumnTypes.SINGLE_SELECT:
        return _sort_single_select(left, right, sort_type, option_index_map)
    if effective_type == ColumnTypes.MULTIPLE_SELECT:
        return _sort_multiple_select(left, right, sort_type, option_index_map)
    if effective_type in (ColumnTypes.COLLABORATOR, ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER):
        return _sort_collaborator(left, right, sort_type)
    if effective_type == ColumnTypes.DEPARTMENT_SINGLE_SELECT:
        return _sort_department(left, right, sort_type)
    return _sort_text(
        _formula_display_string(left, column_data),
        _formula_display_string(right, column_data),
        sort_type,
    )


def _sort_groups(groups, column, sort_type, email2nickname=None):
    column_type = column.get('type')
    option_index_map = {}
    if column_type in (ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT):
        option_index_map = _get_option_name_index_map(column)

    def compare(left, right):
        return _compare_group_values(
            column,
            left.get('_sort_value'),
            right.get('_sort_value'),
            sort_type,
            option_index_map,
            email2nickname,
        )

    groups.sort(key=cmp_to_key(compare))


def generate_groups(db_rows, group_bys, table_metadata, email2nickname=None, first_day_of_week=None):
    if not group_bys:
        return db_rows

    group_bys = group_bys[:MAX_GROUP_LEVEL]
    group_by = group_bys[0]
    column_key = group_by.get('column_key')
    columns = table_metadata.get('columns') or []
    column = next((item for item in columns if item.get('key') == column_key), None)
    if not column:
        return db_rows

    column_name = column.get('name')
    sort_type = group_by.get('sort_type') or 'up'

    groups = []
    group_map = {}
    empty_value_rows = []
    for row in db_rows:
        sort_value, display_value, bucket_key = _normalize_group_cell_value(
            row, column, group_by, email2nickname, first_day_of_week
        )
        if _is_empty(sort_value):
            empty_value_rows.append(row)
            continue
        group = group_map.get(bucket_key)
        if group is None:
            group = {
                'column_key': column_key,
                'column_name': column_name,
                'cell_value': display_value,
                '_sort_value': sort_value,
                'rows': [],
            }
            group_map[bucket_key] = group
            groups.append(group)
        group['rows'].append(row)

    _sort_groups(groups, column, sort_type, email2nickname)

    if empty_value_rows:
        groups.append({
            'column_key': column_key,
            'column_name': column_name,
            'cell_value': None,
            'rows': empty_value_rows,
        })

    for group in groups:
        group.pop('_sort_value', None)

    if group_bys[1:]:
        for group in groups:
            group_rows = group['rows']
            group['rows'] = None
            group['subgroups'] = generate_groups(
                group_rows, group_bys[1:], table_metadata, email2nickname, first_day_of_week
            )
    return groups


def _get_summaries(rows, numeric_columns):
    summaries = {}
    for column in numeric_columns:
        column_name = column.get('name')
        values = []
        for row in rows:
            cell_value = row.get(column_name)
            if isinstance(cell_value, list) and len(cell_value) == 1:
                cell_value = cell_value[0]
            if _is_number(cell_value):
                values.append(cell_value)
        if not values:
            summaries[column_name] = {'sum': 0, 'average': 0, 'median': None, 'max': None, 'min': None}
            continue
        total = sum(values)
        count = len(values)
        sorted_values = sorted(values)
        if count % 2 == 0:
            median = (sorted_values[count // 2 - 1] + sorted_values[count // 2]) / 2
        else:
            median = sorted_values[count // 2]
        summaries[column_name] = {
            'sum': total,
            'average': total / count,
            'median': median,
            'max': max(values),
            'min': min(values),
        }
    return summaries


def _get_group_leaf_rows(group):
    result = []
    for subgroup in group.get('subgroups') or []:
        if subgroup.get('rows'):
            result.extend(subgroup.get('rows'))
        if subgroup.get('subgroups'):
            result.extend(_get_group_leaf_rows(subgroup))
    return result


def _update_group_summaries(groups, numeric_columns):
    for group in groups:
        subgroups = group.get('subgroups')
        rows = group.get('rows')
        if isinstance(subgroups, list) and len(subgroups) > 0:
            _update_group_summaries(subgroups, numeric_columns)
            group['summaries'] = _get_summaries(_get_group_leaf_rows(group), numeric_columns)
        elif rows:
            group['summaries'] = _get_summaries(rows, numeric_columns)


def compute_group_summaries(groups, table_metadata):
    if not isinstance(groups, list):
        return
    columns = table_metadata.get('columns') or []
    numeric_columns = [column for column in columns if _is_numeric_column(column)]
    if not numeric_columns:
        return
    _update_group_summaries(groups, numeric_columns)
