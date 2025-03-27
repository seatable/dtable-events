import os
import time
from datetime import timedelta, datetime

# set timezone
os.environ['TZ'] = 'UTC'
time.tzset()  # 在Unix-like系统上生效

def get_expected_sql_for_modifier(filter_modifier, column_name):
    today = datetime.today()
    week_day = today.isoweekday()
    if filter_modifier == 'the_past_week':
        start_date = today - timedelta(days=(week_day + 6))
        end_date = today - timedelta(days=week_day)
    elif filter_modifier == 'this_week':
        start_date = today - timedelta(days=week_day - 1)
        end_date = today + timedelta(days=7 - week_day)
    elif filter_modifier == 'the_next_week':
        start_date = today + timedelta(days=8 - week_day)
        end_date = today + timedelta(days=14 - week_day)

    start_date = start_date.strftime("%Y-%m-%d")
    end_date = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")
    expected_sql = "SELECT * FROM `Table1` WHERE ((`%s` >= '%s' and `%s` < '%s')) LIMIT 0, 100" % (column_name, start_date, column_name, end_date)
    return expected_sql



TEST_CONDITIONS = [
    # Text / Email / URL Column
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"}
            ],
            "filter_predicate": 'And',
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` = 'LINK') LIMIT 0, 100",
        "by_group": False,

    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'contains', 'filter_term': "LINK"}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` ilike '%LINK%') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'does_not_contain', 'filter_term': "LINK"}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` not ilike '%LINK%') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'is_not', 'filter_term': "LINK"}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` <> 'LINK') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'is_empty'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` is null) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'is_not_empty'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`名称` is not null) LIMIT 0, 100",
        "by_group": False,
    },

    # AutoNumber column
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'AutoNo', 'filter_predicate': 'is', 'filter_term': 'NO-001'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`AutoNo` = 'NO-001') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'AutoNo', 'filter_predicate': 'is_not', 'filter_term': 'NO-001'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`AutoNo` <> 'NO-001') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'AutoNo', 'filter_predicate': 'contains', 'filter_term': 'NO'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`AutoNo` ilike '%NO%') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'AutoNo', 'filter_predicate': 'does_not_contain', 'filter_term': 'NO'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`AutoNo` not ilike '%NO%') LIMIT 0, 100",
        "by_group": False,
    },

    # Number / Duration / Rate column
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Num', 'filter_predicate': 'equal', 'filter_term': '5'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Num` = 5) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'rate', 'filter_predicate': 'greater', 'filter_term': 6}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`rate` > 6) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Du', 'filter_predicate': 'less_or_equal', 'filter_term': '1:30'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Du` <= 5400) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Du', 'filter_predicate': 'greater_or_equal', 'filter_term': '0:20'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Du` >= 1200) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Du', 'filter_predicate': 'not_equal', 'filter_term': '3800'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Du` <> 3800) LIMIT 0, 100",
        "by_group": False,
    },

    # Date / CTIME / MTIME
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is_before', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`Time2d` < '2021-12-20' and `Time2d` is not null)) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`Time2d` >= '2021-12-20' and `Time2d` < '2021-12-21')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is_within', 'filter_term': '', 'filter_term_modifier':'this_week'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": get_expected_sql_for_modifier('this_week', 'Time2d'),
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is_within', 'filter_term': '', 'filter_term_modifier':'the_past_week'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": get_expected_sql_for_modifier('the_past_week', 'Time2d'),
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is_within', 'filter_term': '', 'filter_term_modifier':'the_next_week'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": get_expected_sql_for_modifier('the_next_week', 'Time2d'),
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is_not', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`Time2d` >= '2021-12-21' or `Time2d` < '2021-12-20' or `Time2d` is null)) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'createTime', 'filter_predicate': 'is_on_or_before', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`createTime` < '2021-12-21T00:00:00.000000+00:00' and `createTime` is not null)) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'createTime', 'filter_predicate': 'is_on_or_after', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`createTime` >= '2021-12-20T00:00:00.000000+00:00' and `createTime` is not null)) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'modifyTime', 'filter_predicate': 'is_after', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE ((`modifyTime` >= '2021-12-21T00:00:00.000000+00:00' and `modifyTime` is not null)) LIMIT 0, 100",
        "by_group": False,
    },

    # Single select
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is', 'filter_term': '63347'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Sing` = 'a') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_not', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_not', 'filter_term': '905189'}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Sing` <> 'b') LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_any_of', 'filter_term': ['63347','905189']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Sing` in ('a', 'b')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_none_of', 'filter_term': ['63347','905189', '506341']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Sing` not in ('a', 'b', 'c')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_any_of', 'filter_term': []}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Sing', 'filter_predicate': 'is_none_of', 'filter_term': []}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },

    # Multiple Select / Collaborator
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Mul', 'filter_predicate': 'has_any_of', 'filter_term': ['885435','764614', '418530', '634546']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Mul` in ('aa', 'bb', 'cc', 'dd')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Mul', 'filter_predicate': 'has_all_of', 'filter_term': ['885435','764614', '418530']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Mul` has all of ('aa', 'bb', 'cc')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Mul', 'filter_predicate': 'has_none_of', 'filter_term': ['885435','764614', '418530']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Mul` has none of ('aa', 'bb', 'cc')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Mul', 'filter_predicate': 'is_exactly', 'filter_term': ['885435','764614']}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Mul` is exactly ('aa', 'bb')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Colla', 'filter_predicate': 'is_exactly', 'filter_term': ["87d485c2281a42adbddb137a1070f395@auth.local"]}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Colla` is exactly ('87d485c2281a42adbddb137a1070f395@auth.local')) LIMIT 0, 100",
        "by_group": False,
    },

    # Creator / LastModifier
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Creator', 'filter_predicate': 'contains', 'filter_term': "87d485c2281a42adbddb137a1070f395@auth.local"}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Creator` in ('87d485c2281a42adbddb137a1070f395@auth.local')) LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Modify', 'filter_predicate': 'does_not_contain', 'filter_term': ["87d485c2281a42adbddb137a1070f395@auth.local","xxx"]}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` WHERE (`Modify` not in ('87d485c2281a42adbddb137a1070f395@auth.local', 'xxx')) LIMIT 0, 100",
        "by_group": False,
    },

    # ignore conditions
    ## filter-term incomplete
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Num', 'filter_predicate': 'equal', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    ### filter-term imcomplete
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term_modifier': 'number_of_days_ago', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term_modifier': 'number_of_days_from_now', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term_modifier': 'the_next_numbers_of_days', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term_modifier': 'the_past_numbers_of_days', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    ## filter-predicate incomplete
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Num', 'filter_term': ''}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },
    ## date has no modifier
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'Time2d', 'filter_predicate': 'is', 'filter_term': 1}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql": "SELECT * FROM `Table1` LIMIT 0, 100",
        "by_group": False,
    },

    # Query Combination
    {
        "filter_conditions": {
            "filters": [
                {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"},
                {'column_name': 'AutoNo', 'filter_predicate': 'contains', 'filter_term': 'NO'},
                {'column_name': 'rate', 'filter_predicate': 'greater', 'filter_term': 6},
                {'column_name': 'Mul', 'filter_predicate': 'has_any_of', 'filter_term': ['885435','764614', '418530', '634546']},
                {'column_name': 'modifyTime', 'filter_predicate': 'is_after', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'},
                {'column_name': 'Sing', 'filter_predicate': 'is_none_of', 'filter_term': ['63347','905189', '506341']}

            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "expected_sql":"SELECT * FROM `Table1` WHERE (`名称` = 'LINK') And (`AutoNo` ilike '%NO%') And (`rate` > 6) And (`Mul` in ('aa', 'bb', 'cc', 'dd')) And ((`modifyTime` >= '2021-12-21T00:00:00.000000+00:00' and `modifyTime` is not null)) And (`Sing` not in ('a', 'b', 'c')) LIMIT 0, 100",
        "by_group": False,
    },


    # Group Query
    {
        "filter_conditions":{
            "filter_groups":[
                {
                    "filters":[
                        {'column_name': '名称', 'filter_predicate': 'is', 'filter_term': "LINK"},
                        {'column_name': 'AutoNo', 'filter_predicate': 'contains', 'filter_term': 'NO'},
                    ],
                    "filter_conjunction": 'And'
                },
                {
                    "filters":[
                        {'column_name': 'rate', 'filter_predicate': 'greater', 'filter_term': 6},
                        {'column_name': 'Mul', 'filter_predicate': 'has_any_of', 'filter_term': ['885435','764614', '418530', '634546']},
                        {'column_name': 'modifyTime', 'filter_predicate': 'is_after', 'filter_term': '2021-12-20', 'filter_term_modifier':'exact_date'},
                    ],
                    "filter_conjunction": 'Or'
                },
            ],
            "group_conjunction": 'Or',
            "sorts":[
                {'column_name': '名称'},
                {'column_name': 'AutoNo', 'sort_type': 'up'}
            ],
            "limit": 500
        },
        "by_group": True,
        "expected_sql":"SELECT * FROM `Table1` WHERE ((`名称` = 'LINK') And (`AutoNo` ilike '%NO%')) Or ((`rate` > 6) Or (`Mul` in ('aa', 'bb', 'cc', 'dd')) Or ((`modifyTime` >= '2021-12-21T00:00:00.000000+00:00' and `modifyTime` is not null))) ORDER BY `名称` DESC, `AutoNo` ASC LIMIT 0, 500"
    },

    # Not group, column not found raise Exception
    {
        "filter_conditions": {
            "filters": [
                {'column_name': 'not exists', 'filter_predicate': 'is', 'filter_term': 1}
            ],
            "filter_predicate": 'And',
            "sorts":[],
        },
        "by_group": False,
        "expected_error": ValueError,
    },

    # Group, column not found raise Exception
    {
        "filter_conditions":{
            "filter_groups":[
                {
                    "filters":[
                        {'column_name': 'not exists', 'filter_predicate': 'is', 'filter_term': "LINK"},
                    ],
                    "filter_conjunction": 'And'
                },
            ],
            "group_conjunction": 'Or',
            "sorts":[
                {'column_name': '名称'},
                {'column_name': 'AutoNo', 'sort_type': 'up'}
            ],
            "limit": 500
        },
        "by_group": True,
        "expected_error": ValueError
    },
]

TEST_CONDITIONS_LINK = [
    {
        'row_ids': ["ZBdik2Q2RlWu0BeKVyBtWQ"],
        "expected_sql": "SELECT * FROM `Table2` WHERE `_id` in ('ZBdik2Q2RlWu0BeKVyBtWQ')"
    }
]
