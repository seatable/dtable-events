import unittest
import os
import sys
sys.path.append(sys.path.append(os.path.join(os.path.dirname(__file__), '../')))
d = os.path.dirname
sys.path.append(sys.path.append(d(d(d(d(__file__))))))
from sql.column_reference import TEST_COLUMNS
from sql.test_reference import TEST_CONDITIONS
from dtable_events import filter2sql

class SqlTest(unittest.TestCase):

    table_name = 'Table1'

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
            by_group = conditions.get('by_group')
            sql = self._toSql(filter_conditions, by_group=by_group)
            self.assertEqual(sql, expected_sql)


if __name__ == '__main__':
    unittest.main()
