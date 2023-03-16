import jwt
import time
import logging
import requests
import re
from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

from dtable_events.app.config import DTABLE_PRIVATE_KEY, INNER_DTABLE_DB_URL
from dtable_events.utils import uuid_str_to_36_chars
from dtable_events.utils.constants import FilterPredicateTypes, FormulaResultType, FilterTermModifier, ColumnTypes, \
    DurationFormatsType, StatisticType
from dtable_events.utils.dtable_column_utils import is_numeric_column, is_date_column

logger = logging.getLogger(__name__)

DTABLE_DB_SUMMARY_METHOD = {
    'MEAN': 'AVG',
    'MAX': 'MAX',
    'SUM': 'SUM',
    'MIN': 'MIN',
    'COUNT': 'COUNT'
}

class Operator(object):

    def __init__(self, column, filter_item):
        self.column = column
        self.filter_item = filter_item

        self.column_name = ''
        self.filter_term = ''

        self.filter_predicate = ''
        self.filter_term_modifier = ''
        self.column_type = ''
        self.column_data = {}

        self.init()

    def init(self):
        self.column_name = self.column.get('name', '')
        self.column_type = self.column.get('type', '')
        self.column_data = self.column.get('data', {})
        self.filter_predicate = self.filter_item.get('filter_predicate', '')
        self.filter_term = self.filter_item.get('filter_term', '')
        self.filter_term_modifier = self.filter_item.get('filter_term_modifier', '')
        self.case_sensitive = self.filter_item.get('case_sensitive', False)

    def op_is(self):
        if not self.filter_term:
            return ""
        return "`%s` %s '%s'" % (
            self.column_name,
            '=',
            self.filter_term
        )

    def op_is_not(self):
        if not self.filter_term:
            return ""
        return "`%s` %s '%s'" % (
            self.column_name,
            '<>',
            self.filter_term
        )

    def op_contains(self):
        if not self.filter_term:
            return ""
        return "`%s` %s '%%%s%%'" % (
            self.column_name,
            'like' if self.case_sensitive is True else 'ilike',
            self.filter_term.replace('\\', '\\\\') # special characters require translation
        )

    def op_does_not_contain(self):
        if not self.filter_term:
            return ''
        return "`%s` %s '%%%s%%'" % (
            self.column_name,
            'not like' if self.case_sensitive is True else 'not ilike',
            self.filter_term.replace('\\', '\\\\') # special characters require translation
        )

    def op_equal(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` = %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_not_equal(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` <> %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_less(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` < %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_less_or_equal(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` <= %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_greater(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` > %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_greater_or_equal(self):
        if self.filter_term is None:
            return ''
        return "`%(column_name)s` >= %(value)s" % ({
            'column_name': self.column_name,
            'value': self.filter_term
        })

    def op_is_empty(self):
        return "`%(column_name)s` is null" % ({
            'column_name': self.column_name
        })

    def op_is_not_empty(self):
        return "`%(column_name)s` is not null" % ({
            'column_name': self.column_name
        })

    def op_is_current_user_id(self):
        if not self.filter_term:
            return "(`%s`IS NULL AND `%s` IS NOT NULL)" % (
                self.column_name,
                self.column_name
            )
        return "`%s` %s '%s'" % (
            self.column_name,
            '=',
            self.filter_term
        )


class TextOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.CONTAINS,
        FilterPredicateTypes.NOT_CONTAIN,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
        FilterPredicateTypes.IS_CURRENT_USER_ID,
    ]

    def __init__(self, column, filter_item):
        super(TextOperator, self).__init__(column, filter_item)


class NumberOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.EQUAL,
        FilterPredicateTypes.NOT_EQUAL,
        FilterPredicateTypes.GREATER,
        FilterPredicateTypes.GREATER_OR_EQUAL,
        FilterPredicateTypes.LESS,
        FilterPredicateTypes.LESS_OR_EQUAL,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(NumberOperator, self).__init__(column, filter_item)
        if self.column_type == ColumnTypes.DURATION:
            self.filter_term = self._duration2number()

    def _duration2number(self):
        filter_term = self.filter_term
        column_data = self.column.get('data', {})
        if filter_term == 0 or filter_term == '0':
            return 0
        if not filter_term:
            return ''

        duration_format = column_data.get('duration_format')
        if not duration_format in [
            DurationFormatsType.H_MM,
            DurationFormatsType.H_MM_SS,
            DurationFormatsType.H_MM_SS_S,
            DurationFormatsType.H_MM_SS_SS,
            DurationFormatsType.H_MM_SS_SSS
        ]:
            return ''
        try:
            return int(filter_term)
        except:
            duration_str = filter_term

        is_negtive = duration_str[0] == '-'
        duration_time = duration_str
        if is_negtive:
            duration_time = duration_str[1:]

        duration_time_split_list = re.split('[:：]', duration_time)
        hours, minutes, seconds = 0, 0, 0
        if duration_format == DurationFormatsType.H_MM:
            try:
                hours = int(duration_time_split_list[0])
            except:
                hours = 0
            try:
                minutes = int(duration_time_split_list[1])
            except:
                minutes = 0

        else:
            try:
                hours = int(duration_time_split_list[0])
            except:
                hours = 0
            try:
                minutes = int(duration_time_split_list[1])
            except:
                minutes = 0
            try:
                seconds = int(duration_time_split_list[2])
            except:
                seconds = 0

        if (not hours) and (not minutes) and (not seconds):
            return ''

        total_time = 3600 * hours + 60 * minutes + seconds
        return -total_time if is_negtive else total_time



class SingleSelectOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS_ANY_OF,
        FilterPredicateTypes.IS_NONE_OF,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(SingleSelectOperator, self).__init__(column, filter_item)

    def _get_option_name_by_id(self, option_id):
        options = self.column.get('data', {}).get('options', [])
        for op in options:
            if op.get('id') == option_id:
                return op.get('name')
        return ''

    def op_is(self):
        filter_term = self._get_option_name_by_id(self.filter_term)
        if not filter_term:
            return ''
        return "`%s` %s '%s'" % (
            self.column_name,
            '=',
            filter_term
        )

    def op_is_not(self):
        filter_term = self._get_option_name_by_id(self.filter_term)
        if not filter_term:
            return ''
        return "`%s` %s '%s'" % (
            self.column_name,
            '<>',
            filter_term
        )

    def op_is_any_of(self):
        filter_term = self.filter_term
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        filter_term = [self._get_option_name_by_id(f) for f in filter_term]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        if not option_names:
            return ""
        return "`%(column_name)s` in (%(option_names)s)" % ({
            "column_name": self.column_name,
            "option_names": ", ".join(option_names)
        })

    def op_is_none_of(self):
        filter_term = self.filter_term
        if not filter_term:
            return ''
        if not isinstance(filter_term, list):
            filter_term = [filter_term, ]
        filter_term = [self._get_option_name_by_id(f) for f in filter_term]
        option_names = ["'%s'" % (op_name) for op_name in filter_term]
        if not option_names:
            return ""
        return "`%(column_name)s` not in (%(option_names)s)" % ({
            "column_name": self.column_name,
            "option_names": ", ".join(option_names)
        })


class MultipleSelectOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.HAS_ANY_OF,
        FilterPredicateTypes.HAS_NONE_OF,
        FilterPredicateTypes.HAS_ALL_OF,
        FilterPredicateTypes.IS_EXACTLY,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]

    def __init__(self, column, filter_item):
        super(MultipleSelectOperator, self).__init__(column, filter_item)

    def _get_option_name_by_id(self, option_id):
        options = self.column.get('data', {}).get('options', [])
        if not options:
            return option_id
        for op in options:
            if op.get('id') == option_id:
                return op.get('name')
        return option_id

    def op_has_any_of(self):
        if not self.filter_term:
            return ""
        filter_term = [self._get_option_name_by_id(f) for f in self.filter_term]
        option_names = ["'%s'" % op_name for op_name in filter_term]
        option_names_str = ', '.join(option_names)
        return "`%(column_name)s` in (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_has_none_of(self):
        if not self.filter_term:
            return ""
        filter_term = [self._get_option_name_by_id(f) for f in self.filter_term]
        option_names = ["'%s'" % op_name for op_name in filter_term]
        option_names_str = ', '.join(option_names)
        return "`%(column_name)s` has none of (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_has_all_of(self):
        if not self.filter_term:
            return ""
        filter_term = [self._get_option_name_by_id(f) for f in self.filter_term]
        option_names = ["'%s'" % op_name for op_name in filter_term]
        option_names_str = ', '.join(option_names)
        return "`%(column_name)s` has all of (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })

    def op_is_exactly(self):
        if not self.filter_term:
            return ""
        filter_term = [self._get_option_name_by_id(f) for f in self.filter_term]
        option_names = ["'%s'" % op_name for op_name in filter_term]
        option_names_str = ', '.join(option_names)
        return "`%(column_name)s` is exactly (%(option_names_str)s)" % ({
            "column_name": self.column_name,
            "option_names_str": option_names_str
        })


class DateOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.IS_AFTER,
        FilterPredicateTypes.IS_BEFORE,
        FilterPredicateTypes.IS_ON_OR_BEFORE,
        FilterPredicateTypes.IS_ON_OR_AFTER,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
        FilterPredicateTypes.IS_WITHIN,
    ]


    def __init__(self, column, filter_item):
        super(DateOperator, self).__init__(column, filter_item)

    def _get_end_day_of_month(self, year, month):
        days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
            days[1] = 29

        return days[month - 1]

    def _format_date(self, dt):
        if dt:
            return dt.strftime("%Y-%m-%d")

    def _other_date(self):
        filter_term_modifier = self.filter_term_modifier
        filter_term = self.filter_term
        today = datetime.today()
        year = today.year

        if filter_term_modifier == FilterTermModifier.TODAY:
            return today, None

        if filter_term_modifier == FilterTermModifier.TOMORROW:
            tomorrow = today + timedelta(days=1)
            return tomorrow, None

        if filter_term_modifier == FilterTermModifier.YESTERDAY:
            yesterday = today - timedelta(days=1)
            return yesterday, None

        if filter_term_modifier == FilterTermModifier.ONE_WEEK_AGO:
            one_week_ago = today - timedelta(days=7)
            return one_week_ago, None

        if filter_term_modifier == FilterTermModifier.ONE_WEEK_FROM_NOW:
            one_week_from_now = today + timedelta(days=7)
            return one_week_from_now, None

        if filter_term_modifier == FilterTermModifier.ONE_MONTH_AGO:
            one_month_ago = today - relativedelta(months=1)
            return one_month_ago, None

        if filter_term_modifier == FilterTermModifier.ONE_MONTH_FROM_NOW:
            one_month_from_now = today + relativedelta(months=1)
            return one_month_from_now, None

        if filter_term_modifier == FilterTermModifier.NUMBER_OF_DAYS_AGO:
            try:
                filter_term = int(filter_term)
            except:
                logger.debug("filter_term is invalid, please assign an integer value of days to filter_term")
                return None, None
            days_ago = today - timedelta(days=filter_term)
            return days_ago, None

        if filter_term_modifier == FilterTermModifier.NUMBER_OF_DAYS_FROM_NOW:
            try:
                filter_term = int(filter_term)
            except:
                logger.debug("filter_term is invalid, please assign an integer value of days to filter_term")
                return None, None
            days_after = today + timedelta(days=filter_term)
            return days_after, None

        if filter_term_modifier == FilterTermModifier.EXACT_DATE:
            try:
                return datetime.strptime(filter_term, "%Y-%m-%d").date(), None
            except:
                logger.debug("filter_term is invalid, please assign an date value to filter_term, such as YYYY-MM-DD")
                return None, None

        if filter_term_modifier == FilterTermModifier.THE_PAST_WEEK:
            week_day = today.isoweekday()  # 1-7
            start_date = today - timedelta(days=(week_day + 6))
            end_date = today - timedelta(days=week_day - 1)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THIS_WEEK:
            week_day = today.isoweekday()
            start_date = today - timedelta(days=week_day - 1)
            end_date = today + timedelta(days=8 - week_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_WEEK:
            week_day = today.isoweekday()
            start_date = today + timedelta(days=8 - week_day)
            end_date = today + timedelta(days=15 - week_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_PAST_MONTH:
            one_month_ago = today - relativedelta(months=1)
            one_month_ago_year = one_month_ago.year
            one_month_ago_month = one_month_ago.month
            one_month_age_end_day = self._get_end_day_of_month(one_month_ago_year, one_month_ago_month)
            start_date = datetime(one_month_ago_year, one_month_ago_month, 1)
            end_date = datetime(one_month_ago_year, one_month_ago_month, one_month_age_end_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THIS_MONTH:
            current_month = today.month
            current_year = today.year
            current_month_end_day = self._get_end_day_of_month(current_year, current_month)
            start_date = datetime(current_year, current_month, 1)
            end_date = datetime(current_year, current_month, current_month_end_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_MONTH:
            next_month = today + relativedelta(months=1)
            next_month_year = next_month.year
            next_month_month = next_month.month
            next_month_end_day = self._get_end_day_of_month(next_month_year, next_month_month)
            start_date = datetime(next_month_year, next_month_month, 1)
            end_date = datetime(next_month_year, next_month_month, next_month_end_day)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_PAST_YEAR:
            last_year = year - 1
            start_date = datetime(last_year, 1, 1)
            end_date = datetime(last_year, 12, 31)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THIS_YEAR:
            start_date = datetime(year, 1, 1)
            end_date = datetime(year, 12, 31)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_YEAR:
            next_year = year + 1
            start_date = datetime(next_year, 1, 1)
            end_date = datetime(next_year, 12, 31)
            return start_date, end_date

        if filter_term_modifier == FilterTermModifier.THE_NEXT_NUMBERS_OF_DAYS:
            try:
                filter_term = int(filter_term)
            except:
                logger.debug("filter_term is invalid, please assign an integer value of days to filter_term")
                return None, None
            end_date = today + timedelta(days=filter_term)
            return today, end_date

        if filter_term_modifier == FilterTermModifier.THE_PAST_NUMBERS_OF_DAYS:
            try:
                filter_term = int(filter_term)
            except:
                logger.debug("filter_term is invalid, please assign an integer value of days to filter_term")
                return None, None
            start_date = today - timedelta(days=filter_term)
            return start_date, today

        return None, None

    def op_is(self):
        date, _ = self._other_date()
        if not date:
            return ""
        next_date = self._format_date(date + timedelta(days=1))
        target_date = self._format_date(date)
        return "`%(column_name)s` >= '%(target_date)s' and `%(column_name)s` < '%(next_date)s'" % ({
            "column_name": self.column_name,
            "target_date": target_date,
            "next_date": next_date
        })

    def op_is_within(self):
        start_date, end_date = self._other_date()
        if not (start_date, end_date):
            return ""
        return "`%(column_name)s` >= '%(start_date)s' and `%(column_name)s` <= '%(end_date)s'" % ({
            "column_name": self.column_name,
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date)
        })

    def op_is_before(self):
        target_date, _ = self._other_date()
        if not target_date:
            return ""
        return "`%(column_name)s` < '%(target_date)s' and `%(column_name)s` is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_after(self):
        target_date, _ = self._other_date()
        if not target_date:
            return ""
        return "`%(column_name)s` > '%(target_date)s'" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_on_or_before(self):
        target_date, _ = self._other_date()
        if not target_date:
            return ""
        return "`%(column_name)s` <= '%(target_date)s' and `%(column_name)s` is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_on_or_after(self):
        target_date, _ = self._other_date()
        if not target_date:
            return ""
        return "`%(column_name)s` >= '%(target_date)s' and `%(column_name)s` is not null" % ({
            "column_name": self.column_name,
            "target_date": self._format_date(target_date)
        })

    def op_is_not(self):
        target_date, _ = self._other_date()
        if not target_date:
            return ""
        start_date = target_date - timedelta(days=1)
        end_date = target_date + timedelta(days=1)
        return "(`%(column_name)s` >= '%(end_date)s' or `%(column_name)s` <= '%(start_date)s') and `%(column_name)s` is not null" % (
        {
            "column_name": self.column_name,
            "start_date": self._format_date(start_date),
            "end_date": self._format_date(end_date)
        })

class CheckBoxOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.IS,
    ]

    def op_is(self):
        if not self.filter_term:
            return "(`%(column_name)s` = %(value)s or `%(column_name)s` is null)" % ({
                "column_name": self.column_name,
                "value": self.filter_term
        })

        return "`%(column_name)s` = %(value)s" % ({
            "column_name": self.column_name,
            "value": self.filter_term
        })

class CollaboratorOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.HAS_ALL_OF,
        FilterPredicateTypes.IS_EXACTLY,
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
        FilterPredicateTypes.HAS_ANY_OF,
        FilterPredicateTypes.HAS_NONE_OF,
        FilterPredicateTypes.INCLUDE_ME,
    ]

    def op_has_any_of(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ""
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        collaborator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(collaborator_list)
        return "`%(column_name)s` in (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_has_all_of(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ""
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        collaborator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(collaborator_list)
        return "`%(column_name)s` has all of (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_has_none_of(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ""
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        collaborator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(collaborator_list)
        return "`%(column_name)s` has one of (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_is_exactly(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ""
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        collaborator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(collaborator_list)
        return "`%(column_name)s` is exactly (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_include_me(self):
        return self.op_has_any_of()

class CreatorOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.CONTAINS,
        FilterPredicateTypes.NOT_CONTAIN,
        FilterPredicateTypes.IS,
        FilterPredicateTypes.IS_NOT,
        FilterPredicateTypes.INCLUDE_ME,
    ]

    def op_contains(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ''
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        creator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        filter_term_str = ", ".join(creator_list)
        return "`%(column_name)s` in (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": filter_term_str
        })

    def op_does_not_contain(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ''
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        creator_list = ["'%s'" % collaborator for collaborator in select_collaborators]
        return "`%(column_name)s` not in (%(filter_term_str)s)" % ({
            "column_name": self.column_name,
            "filter_term_str": ', '.join(creator_list)
        })

    def op_include_me(self):
        select_collaborators = self.filter_term
        if not select_collaborators:
            return ''
        if not isinstance(select_collaborators, list):
            select_collaborators = [select_collaborators, ]
        creator = select_collaborators[0] if select_collaborators else ''
        return "%s %s '%s'" % (
            self.column_name,
            '=',
            creator
        )

class FileOperator(Operator):
    SUPPORT_FILTER_PREDICATE = [
        FilterPredicateTypes.EMPTY,
        FilterPredicateTypes.NOT_EMPTY,
    ]
    def __init__(self, column, filter_item):
        super(FileOperator, self).__init__(column, filter_item)


class ArrayOperator(object):

    def __new__(cls, column, filter_item):
        column_data = column.get('data', {})
        column_name = column.get('name', '')
        array_type, array_data = column_data.get('array_type', ''), column_data.get('array_data')
        linked_column = {
            'name': column_name,
            'type': array_type,
            'data': array_data
        }


        if array_type == FormulaResultType.STRING:
            new_column = {
                'name': column_name,
                'type': ColumnTypes.TEXT,
            }
            return TextOperator(new_column, filter_item)

        if array_type == FormulaResultType.BOOL:
            new_column = {
                'name': column_name,
                'type': ColumnTypes.CHECKBOX,
            }
            return CheckBoxOperator(new_column, filter_item)

        if array_type == ColumnTypes.SINGLE_SELECT:
            return MultipleSelectOperator(linked_column, filter_item)

        if array_type in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            return CollaboratorOperator(linked_column, filter_item)

        operator = _get_operator_by_type(array_type)
        return operator(linked_column, filter_item)

class FormulaOperator(object):
    def __new__(cls, column, filter_item):
        column_data = column.get('data', {})
        column_name = column.get('name', '')
        result_type = column_data.get('result_type')
        if result_type == FormulaResultType.STRING:
            new_column = {
                "name": column_name,
                "type": ColumnTypes.TEXT
            }
            return TextOperator(new_column, filter_item)

        if result_type == FormulaResultType.BOOL:
            return CheckBoxOperator(column, filter_item)

        if result_type == FormulaResultType.DATE:
            return DateOperator(column, filter_item)

        if result_type == FormulaResultType.NUMBER:
            return NumberOperator(column, filter_item)

        if result_type == FormulaResultType.ARRAY:
            return ArrayOperator(column,filter_item)

        return None


def _filter2sqlslice(operator):
    support_filter_predicates = operator.SUPPORT_FILTER_PREDICATE
    filter_predicate = operator.filter_predicate
    if not operator.filter_predicate in support_filter_predicates:
        raise ValueError(
            "%(column_type)s type column '%(column_name)s' does not support '%(value)s', available predicates are %(available_predicates)s" % (
            {
                'column_type': operator.column_type,
                'column_name': operator.column_name,
                'value': operator.filter_predicate,
                'available_predicates': support_filter_predicates,
            })
        )

    if filter_predicate == FilterPredicateTypes.IS:
        return operator.op_is()
    if filter_predicate == FilterPredicateTypes.IS_NOT:
        return operator.op_is_not()
    if filter_predicate == FilterPredicateTypes.CONTAINS:
        return operator.op_contains()
    if filter_predicate == FilterPredicateTypes.NOT_CONTAIN:
        return operator.op_does_not_contain()
    if filter_predicate == FilterPredicateTypes.EMPTY:
        return operator.op_is_empty()
    if filter_predicate == FilterPredicateTypes.NOT_EMPTY:
        return operator.op_is_not_empty()
    if filter_predicate == FilterPredicateTypes.EQUAL:
        return operator.op_equal()
    if filter_predicate == FilterPredicateTypes.NOT_EQUAL:
        return operator.op_not_equal()
    if filter_predicate == FilterPredicateTypes.GREATER:
        return operator.op_greater()
    if filter_predicate == FilterPredicateTypes.GREATER_OR_EQUAL:
        return operator.op_greater_or_equal()
    if filter_predicate == FilterPredicateTypes.LESS:
        return operator.op_less()
    if filter_predicate == FilterPredicateTypes.LESS_OR_EQUAL:
        return operator.op_less_or_equal()
    if filter_predicate == FilterPredicateTypes.IS_EXACTLY:
        return operator.op_is_exactly()
    if filter_predicate == FilterPredicateTypes.IS_ANY_OF:
        return operator.op_is_any_of()
    if filter_predicate == FilterPredicateTypes.IS_NONE_OF:
        return operator.op_is_none_of()
    if filter_predicate == FilterPredicateTypes.IS_ON_OR_AFTER:
        return operator.op_is_on_or_after()
    if filter_predicate == FilterPredicateTypes.IS_AFTER:
        return operator.op_is_after()
    if filter_predicate == FilterPredicateTypes.IS_ON_OR_BEFORE:
        return operator.op_is_on_or_before()
    if filter_predicate == FilterPredicateTypes.IS_BEFORE:
        return operator.op_is_before()
    if filter_predicate == FilterPredicateTypes.IS_WITHIN:
        return operator.op_is_within()
    if filter_predicate == FilterPredicateTypes.HAS_ALL_OF:
        return operator.op_has_all_of()
    if filter_predicate == FilterPredicateTypes.HAS_ANY_OF:
        return operator.op_has_any_of()
    if filter_predicate == FilterPredicateTypes.HAS_NONE_OF:
        return operator.op_has_none_of()
    if filter_predicate == FilterPredicateTypes.INCLUDE_ME:
        return operator.op_include_me()
    if filter_predicate == FilterPredicateTypes.IS_CURRENT_USER_ID:
        return operator.op_is_current_user_id()
    return ''

def _get_operator_by_type(column_type):

    if column_type in [
        ColumnTypes.TEXT,
        ColumnTypes.URL,
        ColumnTypes.AUTO_NUMBER,
        ColumnTypes.EMAIL,
        ColumnTypes.GEOLOCATION,
    ]:
        return TextOperator

    if column_type in [
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.RATE
    ]:
        return NumberOperator

    if column_type == ColumnTypes.CHECKBOX:
        return CheckBoxOperator

    if column_type in [
        ColumnTypes.DATE,
        ColumnTypes.CTIME,
        ColumnTypes.MTIME
    ]:
        return DateOperator

    if column_type == ColumnTypes.SINGLE_SELECT:
        return SingleSelectOperator

    if column_type == ColumnTypes.MULTIPLE_SELECT:
        return MultipleSelectOperator

    if column_type == ColumnTypes.COLLABORATOR:
        return CollaboratorOperator

    if column_type in [
        ColumnTypes.CREATOR,
        ColumnTypes.LAST_MODIFIER,
    ]:
        return CreatorOperator

    if column_type in [
        ColumnTypes.FILE,
        ColumnTypes.IMAGE,
        ColumnTypes.LONG_TEXT,
    ]:
        return FileOperator


    if column_type == ColumnTypes.LINK:
        return ArrayOperator

    if column_type in [
        ColumnTypes.FORMULA,
        ColumnTypes.LINK_FORMULA,
    ]:
        return FormulaOperator

    return None

class StatisticSQLGenerator(object):

    def __init__(self, table, statistic_type, statistic, username, id_in_org):
        self.error = None
        self.statistic_type = statistic_type
        table_name = table.get('name', '')
        self.table_name = '`%s`' % table_name
        self.statistic = statistic
        
        columns = table.get('columns', [])
        self.column_key_map = {}
        for column in columns:
            self.column_key_map[column['key']] = column

        filters = statistic.get('filters', [])
        if filters:
            for item in filters:
                if item.get('filter_predicate') == 'include_me':
                    item['filter_term'].append(username)
                if item.get('filter_predicate') == 'is_current_user_ID':
                    item['filter_term'] = id_in_org
        self.filters = filters

        filter_conjunction = statistic.get('filter_conjunction', 'and')
        self.filter_conjunction = filter_conjunction.upper()
        self.filter_sql = self._filter_2_sql()

    def _get_column_by_key(self, column_key):
        return self.column_key_map.get(column_key, None)

    def _filter_2_sql(self):
        filter_sql = ''
        if not self.filters:
            return filter_sql
        
        filter_string_list = []
        
        filter_conjunction = " %s " % self.filter_conjunction
        for filter_item in self.filters:
            column_key = filter_item.get('column_key')
            column = column_key and self._get_column_by_key(column_key)
            if column:
                column_type = column.get('type')
                operator = _get_operator_by_type(column_type)(column, filter_item)
                sql_condition = _filter2sqlslice(operator)
                if sql_condition:
                    filter_string_list.append(sql_condition)
        if filter_string_list:
            filter_sql = filter_conjunction.join(filter_string_list)
        return filter_sql
    
    def _update_filter_sql(self, x_axis_include_empty, x_axis_column):
        column_name = x_axis_column.get('name', '')
        if x_axis_include_empty:
            if self.filter_sql:
                self.filter_sql = 'WHERE %s' % self.filter_sql
        else:
            not_include_empty_sql = '`%s` is not null' % column_name
            if self.filter_sql:
                self.filter_sql = 'WHERE %s AND (%s)' % (not_include_empty_sql, self.filter_sql)
            else:
                self.filter_sql = 'WHERE %s' % not_include_empty_sql

    def _statistic_column_name_to_sql(self, column, group_by):
        column_name = column.get('name', '')
        valid_column_name = '`%s`' % column_name
        type = column.get('type', '')
        if type == ColumnTypes.CTIME or type == ColumnTypes.MTIME or type == ColumnTypes.DATE:
            date_granularity = group_by.get('date_granularity', '')
            date_granularity = date_granularity.upper()
            if date_granularity == 'DAY':
                return 'ISODATE(%s)' % column_name
            if date_granularity == 'WEEK':
                return 'ISODATE(STARTOFWEEK(%s, "monday"))' % column_name
            if date_granularity == 'MONTH':
                return 'ISOMONTH(%s)' % column_name
            if date_granularity == 'QUARTER':
                return 'CONCATENATE(year(%s), "-Q", quarter(%s))' % (valid_column_name, valid_column_name)
            if date_granularity == 'YEAR':
                return 'YEAR(%s)' % valid_column_name
            if date_granularity == 'MAX':
                return 'MAX(%s)' % valid_column_name
            if date_granularity == 'MIN':
                return 'MIN(%s)' % valid_column_name
            return 'ISOMONTH(%s)' % column_name
        if type == ColumnTypes.GEOLOCATION:
            geolocation_granularity = group_by.get('geolocation_granularity', '')
            geolocation_granularity = geolocation_granularity.upper()
            if geolocation_granularity == 'PROVINCE':
                return 'PROVINCE(%s)' % (valid_column_name)
            if geolocation_granularity == 'CITY':
                return 'CITY(%s)' % (valid_column_name)
            if geolocation_granularity == 'DISTRICT':
                return 'DISTRICT(%s)' % (valid_column_name)
            return valid_column_name
        else:
            return valid_column_name

    def _summary_column_2_sql(self, summary_method, column):
        column_name = column.get('name', '')
        if summary_method == 'DISTINCT_VALUES':
            return 'COUNT(DISTINCT %s)' % column_name
        if summary_method == 'ROW_COUNT':
            return 'COUNT(%s)' % column_name
        return '%s(`%s`)' % (DTABLE_DB_SUMMARY_METHOD[summary_method], column_name)

    def _basic_statistic_2_sql(self):
        if self.statistic_type in [StatisticType.HORIZONTAL_BAR, StatisticType.HORIZONTAL_GROUP_BAR]:
            x_axis_column_key = self.statistic.get('vertical_axis_column_key', '')
            x_axis_date_granularity = self.statistic.get('vertical_axis_date_granularity', '')
            x_axis_geolocation_granularity = self.statistic.get ('vertical_axis_geolocation_granularity', '')
            x_axis_include_empty_cells = self.statistic.get('vertical_axis_include_empty', False) or False

            y_axis_summary_type = self.statistic.get('horizontal_axis_summary_type', '')
            y_axis_summary_method = self.statistic.get('horizontal_axis_summary_method', '')
            y_axis_summary_column_key = self.statistic.get('horizontal_axis_column_key', '')
        else:
            x_axis_column_key = self.statistic.get('x_axis_column_key', '')
            x_axis_date_granularity = self.statistic.get('x_axis_date_granularity', '')
            x_axis_geolocation_granularity = self.statistic.get('x_axis_geolocation_granularity', '')
            x_axis_include_empty_cells = self.statistic.get('x_axis_include_empty_cells', False) or False

            y_axis_summary_type = self.statistic.get('y_axis_summary_type', '')
            y_axis_summary_method = self.statistic.get('y_axis_summary_method', '')
            y_axis_summary_column_key = self.statistic.get('y_axis_summary_column_key', '')  

        groupby_column = self._get_column_by_key(x_axis_column_key)
        if not groupby_column:
            self.error = 'Group by column not found'
            return ''
        
        self._update_filter_sql(x_axis_include_empty_cells, groupby_column)
        groupby_column_name = self._statistic_column_name_to_sql(groupby_column, {'date_granularity': x_axis_date_granularity, 'geolocation_granularity': x_axis_geolocation_granularity })
        summary_type = y_axis_summary_type.upper()
        summary_column_name = None

        if summary_type == 'COUNT':
            summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
        else:
            summary_column = self._get_column_by_key(y_axis_summary_column_key)
            if summary_column:
                summary_method = y_axis_summary_method.upper()
                summary_column_name = self._summary_column_2_sql(summary_method, summary_column)
        
        if summary_column_name:
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name)
        return 'SELECT %s FROM %s %s GROUP BY %s LIMIT 0, 5000`' % (groupby_column_name, self.table_name, self.filter_sql, groupby_column_name)

    def _grouping_statistic_2_sql(self):

        if self.statistic_type == StatisticType.HORIZONTAL_GROUP_BAR:
            x_axis_column_key = self.statistic.get('vertical_axis_column_key', '')
            x_axis_date_granularity = self.statistic.get('vertical_axis_date_granularity','')
            x_axis_geolocation_granularity = self.statistic.get ('vertical_axis_geolocation_granularity', '')
            x_axis_include_empty_cells = self.statistic.get('vertical_axis_include_empty', False) or False

            y_axis_summary_type = self.statistic.get('horizontal_axis_summary_type', '')
            y_axis_summary_method = self.statistic.get('horizontal_axis_summary_method', '')
            y_axis_summary_column_key = self.statistic.get('horizontal_axis_column_key', '')
        else:
            x_axis_column_key = self.statistic.get('x_axis_column_key', '')
            x_axis_date_granularity = self.statistic.get('x_axis_date_granularity', '')
            x_axis_geolocation_granularity = self.statistic.get('x_axis_geolocation_granularity', '')
            x_axis_include_empty_cells = self.statistic.get('x_axis_include_empty_cells', False) or False

            y_axis_summary_type = self.statistic.get('y_axis_summary_type', '')
            y_axis_summary_method = self.statistic.get('y_axis_summary_method', '')
            y_axis_summary_column_key = self.statistic.get('y_axis_summary_column_key', '') 


        column_groupby_column_key = self.statistic.get('column_groupby_column_key', '')
        column_groupby_date_granularity = self.statistic.get('column_groupby_date_granularity', '')
        column_groupby_geolocation_granularity = self.statistic.get('column_groupby_geolocation_granularity', '')

        column_groupby_multiple_numeric_column = self.statistic.get('column_groupby_multiple_numeric_column', False) or False
        summary_columns = self.statistic.get('summary_columns', []) or []

        groupby_column = self._get_column_by_key(x_axis_column_key)
        if not groupby_column:
            self.error = 'Group by column not found'
            return ''
        
        self._update_filter_sql(x_axis_include_empty_cells, groupby_column)
        groupby_column_name = self._statistic_column_name_to_sql(groupby_column, { 'date_granularity': x_axis_date_granularity, 'geolocation_granularity': x_axis_geolocation_granularity })
        summary_type = y_axis_summary_type.upper()

        if summary_type == 'COUNT':
            column_groupby_column = self._get_column_by_key(column_groupby_column_key)
            if not column_groupby_column:
                return self._basic_statistic_2_sql()
            column_groupby_column_name = self._statistic_column_name_to_sql(column_groupby_column, { 'date_granularity': column_groupby_date_granularity, 'geolocation_granularity': column_groupby_geolocation_granularity })
            summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
            return 'SELECT %s, %s, %s FROM %s %s GROUP BY %s, %s LIMIT 0, 5000' % (groupby_column_name, column_groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name, column_groupby_column_name)
        
        if column_groupby_multiple_numeric_column:
            column_groupby_numeric_columns = summary_columns
            column_groupby_numeric_columns.insert(0, { 'column_key': y_axis_summary_column_key, 'summary_method': y_axis_summary_method })
            column_groupby_numeric_column_names = []
            for summary_column_obj in column_groupby_numeric_columns:
                summary_column_key = summary_column_obj.get('column_key', '')
                summary_method = summary_column_obj.get('summary_method', '').upper()
                if not summary_column_key:
                    continue
                summary_column = self._get_column_by_key(summary_column_key)
                if not summary_column:
                    continue
                summary_column_name = self._summary_column_2_sql(summary_method, summary_column)
                column_groupby_numeric_column_names.append(summary_column_name)
            column_groupby_numeric_column_names_string = ', '.join(column_groupby_numeric_column_names)
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, column_groupby_numeric_column_names_string, self.table_name, self.filter_sql, groupby_column_name)

        summary_column = self._get_column_by_key(y_axis_summary_column_key)
        if not summary_column:
            return self._basic_statistic_2_sql()

        summary_method = y_axis_summary_method.upper()
        column_groupby_column = self._get_column_by_key(column_groupby_column_key)
        summary_column_name = self._summary_column_2_sql(summary_method, summary_column)
        if not column_groupby_column:
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name)
        
        column_groupby_column_name = self._statistic_column_name_to_sql(column_groupby_column, { 'date_granularity': column_groupby_date_granularity, 'geolocation_granularity': column_groupby_geolocation_granularity })
        return 'SELECT %s, %s, %s FROM %s %s GROUP BY %s, %s LIMIT 0, 5000' % (groupby_column_name, column_groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name, column_groupby_column_name)

    def _one_dimension_statistic_table_2_sql(self):
        groupby_column_key = self.statistic.get('groupby_column_key', '')
        summary_type = self.statistic.get('summary_type', '')
        summary_column_key = self.statistic.get('summary_column_key', '')
        groupby_date_granularity = self.statistic.get('groupby_date_granularity', '')
        groupby_geolocation_granularity = self.statistic.get('groupby_geolocation_granularity', '')
        groupby_include_empty_cells = self.statistic.get('groupby_include_empty_cells', False)
        summary_method = self.statistic.get('summary_method', '')
        summary_columns = self.statistic.get('summary_columns', [])
        if not summary_method:
            self.error = 'Summary method is not valid'
            return ''
        
        groupby_column = self._get_column_by_key(groupby_column_key)
        if not groupby_column:
            self.error = 'Group by column not found'
            return ''
        
        self._update_filter_sql(groupby_include_empty_cells, groupby_column)
        groupby_column_name = self._statistic_column_name_to_sql(groupby_column, { 'date_granularity': groupby_date_granularity, 'geolocation_granularity': groupby_geolocation_granularity })
        summary_type = summary_type.upper()

        if summary_type == 'COUNT':
            summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name)

        
        if summary_columns:
            summary_column = self._get_column_by_key(summary_column_key)
            summary_method = summary_method.upper()
            summary_column_names = []
            if summary_column and (is_numeric_column(summary_column) or is_date_column(summary_column)):
                summary_column_name = self._summary_column_2_sql(summary_method, summary_column)
                summary_column_names.append(summary_column_name)
            
            for column_option in summary_columns:
                column_key = column_option.get('column_key', '')
                method = column_option.get('summary_method', '')
                method = method.upper()
                column = self._get_column_by_key(column_key)
                if column and (is_numeric_column(column) or is_date_column(column)):
                    column_name = self._summary_column_2_sql(method, column)
                    summary_column_names.append(column_name)
            
            summary_column_names_str = ', '.join(summary_column_names)
            if summary_column_names_str:
                summary_column_names_str = ', %s' % summary_column_names_str
            
            return 'SELECT %s%s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, summary_column_names_str, self.table_name, self.filter_sql, groupby_column_name)

        summary_method = summary_method.upper()
        new_numeric_column_keys = [ summary_column_key ]
        numeric_column_names = []
        for column_key in new_numeric_column_keys:
            column = self._get_column_by_key(column_key)
            if column and (is_numeric_column(column) or is_date_column(column)):
                column_name = self._summary_column_2_sql(summary_method, column)
                numeric_column_names.append(column_name)

        if numeric_column_names:
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, ', '.join(numeric_column_names), self.table_name, self.filter_sql, groupby_column_name)

        return 'SELECT %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, self.table_name, self.filter_sql, groupby_column_name)

    def _two_dimension_statistic_table_2_sql(self):
        groupby_column_key = self.statistic.get('groupby_column_key', '')
        column_groupby_column_key = self.statistic.get('column_groupby_column_key', '')
        summary_type = self.statistic.get('summary_type', '')
        summary_column_key = self.statistic.get('summary_column_key', '')
        groupby_date_granularity = self.statistic.get('groupby_date_granularity', '')
        groupby_geolocation_granularity = self.statistic.get('groupby_geolocation_granularity', '')
        groupby_include_empty_cells = self.statistic.get('groupby_include_empty_cells', False)
        column_groupby_date_granularity = self.statistic.get('column_groupby_date_granularity', '')
        column_groupby_geolocation_granularity = self.statistic.get('column_groupby_geolocation_granularity', '')
        summary_method = self.statistic.get('summary_method', '')

        column_groupby_column = self._get_column_by_key(column_groupby_column_key)
        if not column_groupby_column:
            return self._one_dimension_statistic_table_2_sql()
        
        groupby_column = self._get_column_by_key(groupby_column_key)
        if not groupby_column:
            self.error = 'Group by column not found'
            return ''

        self._update_filter_sql(groupby_include_empty_cells, groupby_column)
        groupby_column_name = self._statistic_column_name_to_sql(groupby_column, { 'date_granularity': groupby_date_granularity, 'geolocation_granularity': groupby_geolocation_granularity })
        column_groupby_column_name = self._statistic_column_name_to_sql(column_groupby_column, { 'date_granularity': column_groupby_date_granularity, 'geolocation_granularity': column_groupby_geolocation_granularity })

        summary_type = summary_type.upper()

        if summary_type == 'COUNT':
            summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
        else:
            summary_column = self._get_column_by_key(summary_column_key)
            if not summary_column:
                summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
            else:
                if not summary_method:
                    self.error = 'Summary method is not valid'
                    return ''
                summary_method = summary_method.upper()
                summary_column_name = self._summary_column_2_sql(summary_method, summary_column)

        return 'SELECT %s, %s, %s FROM %s %s GROUP BY %s, %s LIMIT 0, 5000' % (groupby_column_name, column_groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name, column_groupby_column_name)

    def _pie_chart_statistic_2_sql(self):
        groupby_column_key = self.statistic.get('groupby_column_key', '')
        summary_type = self.statistic.get('summary_type', '')
        summary_method = self.statistic.get('summary_method', '')
        summary_column_key = self.statistic.get('summary_column_key', '')
        groupby_date_granularity = self.statistic.get('groupby_date_granularity', '')
        groupby_geolocation_granularity = self.statistic.get('groupby_geolocation_granularity', '')
        groupby_include_empty_cells = self.statistic.get('groupby_include_empty_cells', False)
        
        groupby_column = self._get_column_by_key(groupby_column_key)
        if not groupby_column:
            self.error = 'Group by column not found'
            return ''
        
        self._update_filter_sql(groupby_include_empty_cells, groupby_column)
        groupby_column_name = self._statistic_column_name_to_sql(groupby_column, { 'date_granularity': groupby_date_granularity, 'geolocation_granularity': groupby_geolocation_granularity })
        summary_type = summary_type.upper()
        if summary_type == 'COUNT':
            summary_column_name = self._summary_column_2_sql('COUNT', groupby_column)
        else:
            if not summary_method:
                self.error = 'Summary method is not valid'
                return ''
            summary_column = self._get_column_by_key(summary_column_key)
            if summary_column:
                summary_method = summary_method.upper()
                summary_column_name = self._summary_column_2_sql(summary_method, summary_column)
            else:
                summary_column_name = ''
        if summary_column_name:
            return 'SELECT %s, %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, summary_column_name, self.table_name, self.filter_sql, groupby_column_name)
        return 'SELECT %s FROM %s %s GROUP BY %s LIMIT 0, 5000' % (groupby_column_name, self.table_name, self.filter_sql, groupby_column_name)

    def _basic_number_card_chart_statistic_2_sql(self):
        numeric_column_key = self.statistic.get('numeric_column_key', '')
        summary_method = self.statistic.get('summary_method', '')
        numeric_column = self._get_column_by_key(numeric_column_key)
        if not numeric_column:
            self.error = 'Numeric column not found'
            return ''
        if not summary_method:
            self.error = 'Summary method is not valid'
            return ''
        summary_method = summary_method.upper()
        summary_column_name = self._summary_column_2_sql(summary_method, numeric_column)
        return 'SELECT %s FROM %s LIMIT 0, 5000' % (summary_column_name, self.table_name)

    
    def to_sql(self):
        if self.error:
            return '', self.error

        if self.statistic_type in [StatisticType.BAR, StatisticType.LINE, StatisticType.HORIZONTAL_BAR, StatisticType.AREA]:
            sql = self._basic_statistic_2_sql()
            return sql, self.error

        if self.statistic_type in [StatisticType.BAR_GROUP, StatisticType.LINE_GROUP, StatisticType.HORIZONTAL_GROUP_BAR]:
            column_groupby_column_key = self.statistic.get('column_groupby_column_key', '')
            column_groupby_multiple_numeric_column = self.statistic.get('column_groupby_multiple_numeric_column', False) or False
            if not (column_groupby_column_key or column_groupby_multiple_numeric_column):
                sql = self._basic_statistic_2_sql()
                return sql, self.error
            
            sql = self._grouping_statistic_2_sql()
            return sql, self.error

        if self.statistic_type == StatisticType.PIE or self.statistic_type == StatisticType.RING:
            sql = self._pie_chart_statistic_2_sql()
            return sql, self.error

        if self.statistic_type == StatisticType.BASIC_NUMBER_CARD:
            sql = self._basic_number_card_chart_statistic_2_sql()
            return sql, self.error
        
        if self.statistic_type == StatisticType.TABLE:
            column_groupby_column_key = self.statistic.get('column_groupby_column_key', '')
            groupby_column_key = self.statistic.get('groupby_column_key', '')
            if not groupby_column_key:
                return '', 'Groupby column not set'
            if not column_groupby_column_key:
                sql = self._one_dimension_statistic_table_2_sql()
                return sql, self.error
            
            sql = self._two_dimension_statistic_table_2_sql()
            return sql, self.error

        return '', ''



class BaseSQLGenerator(object):

    def __init__(self, table_name, columns, filter_conditions=None, filter_condition_groups=None):
        self.table_name = table_name
        self.filter_conditions = filter_conditions
        self.filter_condition_groups = filter_condition_groups
        self.columns = columns

    def _get_column_by_key(self, col_key):
        for col in self.columns:
            if col.get('key') == col_key:
                return col
        return None

    def _get_column_by_name(self, col_name):
        for col in self.columns:
            if col.get('name') == col_name:
                return col
        return None

    def _sort2sql(self, by_group=False):
        if by_group:
            filter_conditions = self.filter_condition_groups
        else:
            filter_conditions = self.filter_conditions
        condition_sorts = filter_conditions.get('sorts', [])
        if not condition_sorts:
            return ''

        order_header = 'ORDER BY '
        clauses = []
        for sort in condition_sorts:
            column_key = sort.get('column_key', '')
            column_name = sort.get('column_name', '')
            sort_type = sort.get('sort_type', 'DESC') == 'up' and 'ASC' or 'DESC'
            column = self._get_column_by_key(column_key)
            if not column:
                column = self._get_column_by_name(column_name)
                if not column:
                    if column_key in ['_ctime', '_mtime']:
                        order_condition = '%s %s' % (column_key, sort_type)
                        clauses.append(order_condition)
                        continue
                    else:
                        continue

            order_condition = '%s %s' % (column.get('name'), sort_type)
            clauses.append(order_condition)
        if not clauses:
            return ''

        return "%s%s" % (
            order_header,
            ', '.join(clauses)
        )

    def _groupfilter2sql(self):
        filter_condition_groups = self.filter_condition_groups
        filter_groups = filter_condition_groups.get('filter_groups', [])
        group_conjunction = filter_condition_groups.get('group_conjunction', 'And')
        if not filter_groups:
            return ''
        filter_header = 'WHERE '
        group_string_list = []
        group_conjunction_split = ' %s ' % group_conjunction
        for filter_group in filter_groups:
            filters = filter_group.get('filters')
            filter_conjunction = filter_group.get('filter_conjunction', 'And')
            filter_conjunction_split = " %s " % filter_conjunction
            filter_string_list = []
            for filter_item in filters:
                column_key = filter_item.get('column_key')
                column_name = filter_item.get('column_name')
                if not (column_key or column_name):
                    continue
                column = column_key and self._get_column_by_key(column_key)
                if not column:
                    column = column_name and self._get_column_by_name(column_name)
                column_type = column.get('type')
                operator = _get_operator_by_type(column_type)(column, filter_item)
                sql_condition = _filter2sqlslice(operator)
                if not sql_condition:
                    continue
                filter_string_list.append(sql_condition)
            if filter_string_list:
                filter_content = "(%s)" % (
                    filter_conjunction_split.join(filter_string_list)
                )
                group_string_list.append(filter_content)

        return "%s%s" % (
            filter_header,
            group_conjunction_split.join(group_string_list)
        )

    def _filter2sql(self):
        filter_conditions = self.filter_conditions
        filters = filter_conditions.get('filters', [])
        filter_conjunction = filter_conditions.get('filter_conjunction', 'And')
        if not filters:
            return ''

        filter_header = 'WHERE '
        filter_string_list = []
        filter_content = ''
        filter_conjunction_split = " %s " % filter_conjunction
        for filter_item in filters:
            column_key = filter_item.get('column_key')
            column_name = filter_item.get('column_name')
            # skip when the column key or name is missing
            if not (column_key or column_name):
                continue
            column = column_key and self._get_column_by_key(column_key)
            if not column:
                column = column_name and self._get_column_by_name(column_name)
            column_type = column.get('type')
            operator = _get_operator_by_type(column_type)(column, filter_item)
            sql_condition = _filter2sqlslice(operator)
            if not sql_condition:
                continue
            filter_string_list.append(sql_condition)
        if filter_string_list:
            filter_content = "%s" % (
                filter_conjunction_split.join(filter_string_list)
            )
        else:
            return ''
        return "%s%s" % (
            filter_header,
            filter_content
        )

    def _limit2sql(self, by_group=False):
        if by_group:
            filter_conditions = self.filter_condition_groups
        else:
            filter_conditions = self.filter_conditions
        start = filter_conditions.get('start')
        limit = filter_conditions.get('limit')
        limit_clause = '%s %s, %s' % (
            "LIMIT",
            start or 0,
            limit or 100
        )
        return limit_clause

    def to_sql(self, by_group=False):
        sql = "%s `%s`" % (
            "SELECT * FROM",
            self.table_name
        )
        if not by_group:
            filter_clause = self._filter2sql()
            sort_clause = self._sort2sql()
            limit_clause = self._limit2sql()
        else:
            filter_clause = self._groupfilter2sql()
            sort_clause = self._sort2sql(by_group=True)
            limit_clause = self._limit2sql(by_group=True)

        if filter_clause:
            sql = "%s %s" % (sql, filter_clause)
        if sort_clause:
            sql = "%s %s" % (sql, sort_clause)
        if limit_clause:
            sql = "%s %s" % (sql, limit_clause)
        return sql


def filter2sql(table_name, columns, filter_conditions, by_group=False):
    if by_group:
        sql_generator = BaseSQLGenerator(table_name, columns, filter_condition_groups=filter_conditions)
    else:
        sql_generator = BaseSQLGenerator(table_name, columns, filter_conditions=filter_conditions)
    return sql_generator.to_sql(by_group=by_group)


def db_query(dtable_uuid, sql):
    dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
    token = jwt.encode(
        payload={
            'exp': int(time.time()) + 300,
            'dtable_uuid': dtable_uuid,
            'username': 'Automation Rule',
            'permission': 'rw',
        },
        key=DTABLE_PRIVATE_KEY
    )
    if isinstance(token, bytes):
        token = token.decode()

    headers = {'Authorization': 'Token ' + token}
    api_url = INNER_DTABLE_DB_URL.rstrip('/') + '/api/v1/query/' + dtable_uuid + '/'
    params = {
        'sql':sql
    }
    response = requests.post(api_url, json=params, headers=headers)
    try:
        resp_data = response.json()
        success = resp_data.get('success', False)
        if success:
            return resp_data.get('results')
        return []
    except Exception as e:
        logger.error(e)
        return []


def statistic2sql(table, statistic_type, statistic, username='', id_in_org=''):
    sql_generator = StatisticSQLGenerator(table, statistic_type, statistic, username, id_in_org)
    return sql_generator.to_sql()
