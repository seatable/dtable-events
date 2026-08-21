import unittest
import os
import sys
sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '../')))
d = os.path.dirname
sys.path.append(sys.path.append(d(d(d(d(__file__))))))
from sql.column_reference import TEST_COLUMNS, TABLES, LINK_COLUMN
from sql.test_reference import TEST_CONDITIONS, TEST_CONDITIONS_LINK
from dtable_events import filter2sql, linkRecords2sql
from dtable_events.utils.sql_generator import pre_filter_to_filter_term

class SqlTest(unittest.TestCase):

    table_name = 'Table1'

    def __init__(self, methodName = "runTest"):
        super().__init__(methodName)
        self.maxDiff = None

    def _toSql(self, filter_conditions, by_group=False):
        sql = filter2sql(
            self.table_name,
            TEST_COLUMNS,
            filter_conditions,
            by_group=by_group,
        )
        return sql

    def test_equal(self):
        
        for conditions in TEST_CONDITIONS:
            filter_conditions = conditions.get('filter_conditions')
            expected_sql = conditions.get('expected_sql')
            expected_error = conditions.get('expected_error')
            if expected_sql:
                by_group = conditions.get('by_group')
                sql = self._toSql(filter_conditions, by_group=by_group)
                self.assertEqual(sql, expected_sql)
            if expected_error:
                with self.assertRaises(expected_error):
                    by_group = conditions.get('by_group')
                    sql = self._toSql(filter_conditions, by_group=by_group)

        tables = TABLES
        current_table = TABLES[0]
        link_column = LINK_COLUMN
        for condition_l in TEST_CONDITIONS_LINK:
            expected_sql_link = condition_l.get('expected_sql')
            record_ids = condition_l.get('row_ids')
            sql_link = linkRecords2sql(current_table, link_column, record_ids, tables)
            self.assertEqual(sql_link, expected_sql_link)
        



    def test_user_filter_normalization(self):

        def to_sql(filters):
            normalized = pre_filter_to_filter_term(filters, 'me@x.com', 'admin-1', [1, 2], [1, 2, 3])
            return self._toSql({'filters': normalized, 'filter_conjunction': 'And'})

        # collaborator include_me appends current user email
        self.assertEqual(
            to_sql([{'column_name': 'Colla', 'filter_predicate': 'include_me', 'filter_term': ['a@x.com']}]),
            "SELECT * FROM `Table1` WHERE (`Colla` in ('a@x.com', 'me@x.com')) LIMIT 0, 100",
        )
        # text is_current_user_ID replaced with id_in_org
        self.assertEqual(
            to_sql([{'column_name': '名称', 'filter_predicate': 'is_current_user_ID', 'filter_term': ''}]),
            "SELECT * FROM `Table1` WHERE (`名称` = 'admin-1') LIMIT 0, 100",
        )
        # department current_user_department / current_user_department_and_sub
        self.assertEqual(
            to_sql([{'column_name': 'Dept', 'filter_predicate': 'is', 'filter_term': 'current_user_department'}]),
            "SELECT * FROM `Table1` WHERE (`Dept` IN (1, 2)) LIMIT 0, 100",
        )
        self.assertEqual(
            to_sql([{'column_name': 'Dept', 'filter_predicate': 'is_not', 'filter_term': 'current_user_department_and_sub'}]),
            "SELECT * FROM `Table1` WHERE (`Dept` NOT IN (1, 2, 3)) LIMIT 0, 100",
        )
        # nested filter group include_me
        self.assertEqual(
            to_sql([{'filters': [{'column_name': 'Colla', 'filter_predicate': 'include_me', 'filter_term': ['b@x.com']}], 'filter_conjunction': 'And'}]),
            "SELECT * FROM `Table1` WHERE ((`Colla` in ('b@x.com', 'me@x.com'))) LIMIT 0, 100",
        )
        # deeply nested filter group (3 levels) include_me must still be normalized
        self.assertEqual(
            to_sql([
                {
                    'filters': [
                        {
                            'filters': [
                                {'column_name': 'Colla', 'filter_predicate': 'include_me', 'filter_term': ['c@x.com']},
                            ],
                            'filter_conjunction': 'And',
                        },
                    ],
                    'filter_conjunction': 'And',
                },
            ]),
            "SELECT * FROM `Table1` WHERE (((`Colla` in ('c@x.com', 'me@x.com')))) LIMIT 0, 100",
        )
        # deeply nested filter group (3 levels) current_user_department must still be normalized
        self.assertEqual(
            to_sql([
                {
                    'filters': [
                        {
                            'filters': [
                                {'column_name': 'Dept', 'filter_predicate': 'is', 'filter_term': 'current_user_department'},
                            ],
                            'filter_conjunction': 'And',
                        },
                    ],
                    'filter_conjunction': 'And',
                },
            ]),
            "SELECT * FROM `Table1` WHERE (((`Dept` IN (1, 2)))) LIMIT 0, 100",
        )


if __name__ == '__main__':
    unittest.main()
