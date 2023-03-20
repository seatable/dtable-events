class ColumnTypes:
    TEXT = 'text'
    IMAGE = 'image'
    DATE = 'date'
    LONG_TEXT = 'long-text'
    CHECKBOX = 'checkbox'
    SINGLE_SELECT = 'single-select'
    MULTIPLE_SELECT = 'multiple-select'
    URL = 'url'
    DURATION = 'duration'
    NUMBER = 'number'
    FILE = 'file'
    COLLABORATOR = 'collaborator'
    EMAIL = 'email'
    FORMULA = 'formula'
    CREATOR = 'creator'
    LAST_MODIFIER = 'last-modifier'
    AUTO_NUMBER = 'auto-number'
    LINK = 'link'
    CTIME = 'ctime'
    MTIME = 'mtime'
    LINK_FORMULA = 'link-formula'
    RATE = 'rate'
    GEOLOCATION = 'geolocation'
    BUTTON = 'button'


class FilterPredicateTypes(object):
    CONTAINS = 'contains'
    NOT_CONTAIN = 'does_not_contain'
    IS = 'is'
    IS_NOT = 'is_not'
    EQUAL = 'equal'
    NOT_EQUAL = 'not_equal'
    LESS = 'less'
    GREATER = 'greater'
    LESS_OR_EQUAL = 'less_or_equal'
    GREATER_OR_EQUAL = 'greater_or_equal'
    EMPTY = 'is_empty'
    NOT_EMPTY = 'is_not_empty'
    IS_WITHIN = 'is_within'
    IS_BEFORE = 'is_before'
    IS_AFTER = 'is_after'
    IS_ON_OR_BEFORE = 'is_on_or_before'
    IS_ON_OR_AFTER = 'is_on_or_after'
    HAS_ANY_OF = 'has_any_of'
    HAS_ALL_OF = 'has_all_of'
    HAS_NONE_OF = 'has_none_of'
    IS_EXACTLY = 'is_exactly'
    IS_ANY_OF = 'is_any_of'
    IS_NONE_OF = 'is_none_of'
    INCLUDE_ME = 'include_me'
    IS_CURRENT_USER_ID = 'is_current_user_ID'


class FilterTermModifier(object):
    TODAY = 'today'
    TOMORROW = 'tomorrow'
    YESTERDAY = 'yesterday'
    ONE_WEEK_AGO = 'one_week_ago'
    ONE_WEEK_FROM_NOW = 'one_week_from_now'
    ONE_MONTH_AGO = 'one_month_ago'
    ONE_MONTH_FROM_NOW = 'one_month_from_now'
    NUMBER_OF_DAYS_AGO = 'number_of_days_ago'
    NUMBER_OF_DAYS_FROM_NOW = 'number_of_days_from_now'
    EXACT_DATE = 'exact_date'
    THE_PAST_WEEK = 'the_past_week'
    THE_PAST_MONTH = 'the_past_month'
    THE_PAST_YEAR = 'the_past_year'
    THE_NEXT_WEEK = 'the_next_week'
    THE_NEXT_MONTH = 'the_next_month'
    THE_NEXT_YEAR = 'the_next_year'
    THE_NEXT_NUMBERS_OF_DAYS = 'the_next_numbers_of_days'
    THE_PAST_NUMBERS_OF_DAYS = 'the_past_numbers_of_days'
    THIS_WEEK = 'this_week'
    THIS_MONTH = 'this_month'
    THIS_YEAR = 'this_year'


class FormulaResultType(object):
    NUMBER = 'number'
    STRING = 'string'
    DATE = 'date'
    BOOL = 'bool'
    ARRAY = 'array'


class DurationFormatsType(object):
    H_MM = 'h:mm'
    H_MM_SS = 'h:mm:ss'
    H_MM_SS_S = 'h:mm:ss.s'
    H_MM_SS_SS = 'h:mm:ss.ss'
    H_MM_SS_SSS = 'h:mm:ss.sss'


DURATION_ZERO_DISPLAY = {
    DurationFormatsType.H_MM: '0:00',
    DurationFormatsType.H_MM_SS: '0:00',
    DurationFormatsType.H_MM_SS_S: '0:00.0',
    DurationFormatsType.H_MM_SS_SS: '0:00.00',
    DurationFormatsType.H_MM_SS_SSS: '0:00.000'
}

DURATION_DECIMAL_DIGITS = {
    DurationFormatsType.H_MM: 0,
    DurationFormatsType.H_MM_SS: 0,
    DurationFormatsType.H_MM_SS_S: 1,
    DurationFormatsType.H_MM_SS_SS: 2,
    DurationFormatsType.H_MM_SS_SSS: 3
}


ARRAY_FORMAL_COLUMNS = [
    ColumnTypes.IMAGE,
    ColumnTypes.FILE,
    ColumnTypes.MULTIPLE_SELECT,
    ColumnTypes.COLLABORATOR
]


SIMPLE_CELL_FORMULA_RESULTS = [
    FormulaResultType.NUMBER,
    FormulaResultType.STRING,
    FormulaResultType.DATE,
    FormulaResultType.BOOL
]

FORMULA_COLUMN_TYPES = [
    ColumnTypes.FORMULA,
    ColumnTypes.LINK_FORMULA
]

NUMERIC_COLUMNS_TYPES = [
    ColumnTypes.NUMBER,
    ColumnTypes.DURATION,
    ColumnTypes.RATE
]

DATE_COLUMN_TYPES = [
    ColumnTypes.DATE,
    ColumnTypes.CTIME,
    ColumnTypes.MTIME
]

class StatisticType:
    BAR = 'bar'
    BAR_GROUP = 'bar_group'
    PIE = 'pie'
    LINE = 'line'
    LINE_GROUP = 'line_group'
    TABLE = 'table'
    RING = 'ring'
    HORIZONTAL_BAR = 'horizontal_bar'
    HORIZONTAL_GROUP_BAR = 'horizontal_group_bar'
    AREA = 'area'
    BASIC_NUMBER_CARD = 'basic_number_card'
    TREE_MAP = 'tree_map'
    COMBINATION = 'combination'
    DASHBOARD = 'dashboard'


# single/multiple select options
VALID_OPTION_TAGS = [
    {'color': '#FFFCB5', 'border_color': '#E8E79D', 'text_color': '#212529'},
    {'color': '#FFEAB6', 'border_color': '#ECD084', 'text_color': '#212529'},
    {'color': '#FFD9C8', 'border_color': '#EFBAA3', 'text_color': '#212529'},
    {'color': '#FFDDE5', 'border_color': '#EDC4C1', 'text_color': '#212529'},
    {'color': '#FFD4FF', 'border_color': '#E6B6E6', 'text_color': '#212529'},
    {'color': '#DAD7FF', 'border_color': '#C3BEEF', 'text_color': '#212529'},
    {'color': '#DDFFE6', 'border_color': '#BBEBCD', 'text_color': '#212529'},
    {'color': '#DEF7C4', 'border_color': '#C5EB9E', 'text_color': '#212529'},
    {'color': '#D8FAFF', 'border_color': '#B4E4E9', 'text_color': '#212529'},
    {'color': '#D7E8FF', 'border_color': '#BAD1E9', 'text_color': '#212529'},
    {'color': '#B7CEF9', 'border_color': '#96B2E1', 'text_color': '#212529'},
    {'color': '#E9E9E9', 'border_color': '#DADADA', 'text_color': '#212529'},
    {'color': '#FBD44A', 'border_color': '#E5C142', 'text_color': '#FFFFFF'},
    {'color': '#EAA775', 'border_color': '#D59361', 'text_color': '#FFFFFF'},
    {'color': '#F4667C', 'border_color': '#DC556A', 'text_color': '#FFFFFF'},
    {'color': '#DC82D2', 'border_color': '#D166C5', 'text_color': '#FFFFFF'},
    {'color': '#9860E5', 'border_color': '#844BD2', 'text_color': '#FFFFFF'},
    {'color': '#9F8CF1', 'border_color': '#8F75E2', 'text_color': '#FFFFFF'},
    {'color': '#59CB74', 'border_color': '#4EB867', 'text_color': '#FFFFFF'},
    {'color': '#ADDF84', 'border_color': '#9CCF72', 'text_color': '#FFFFFF'},
    {'color': '#89D2EA', 'border_color': '#7BC0D6', 'text_color': '#FFFFFF'},
    {'color': '#4ECCCB', 'border_color': '#45BAB9', 'text_color': '#FFFFFF'},
    {'color': '#46A1FD', 'border_color': '#3C8FE4', 'text_color': '#FFFFFF'},
    {'color': '#C2C2C2', 'border_color': '#ADADAD', 'text_color': '#FFFFFF'},
]
