import unittest
import os
import sys
d = os.path.dirname
sys.path.append(d(d(d(d(__file__)))))
from dtable_events.utils.sql_generator import StatisticSQLGenerator


def _make_generator(filter_sql=''):
    gen = StatisticSQLGenerator.__new__(StatisticSQLGenerator)
    gen.filter_sql = filter_sql
    return gen


class UpdateFilterSqlTest(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None
        self.col_a = {'name': 'colA', 'type': 'text'}
        self.col_b = {'name': 'colB', 'type': 'number'}

    def test_empty_include_empty(self):
        gen = _make_generator('')
        gen._update_filter_sql(True, self.col_a)
        self.assertEqual(gen.filter_sql, '')

    def test_empty_not_include_empty(self):
        gen = _make_generator('')
        gen._update_filter_sql(False, self.col_a)
        self.assertEqual(gen.filter_sql, 'WHERE `colA` is not null')

    def test_first_call_include_empty(self):
        gen = _make_generator('`colA` = "x"')
        gen._update_filter_sql(True, self.col_a)
        self.assertEqual(gen.filter_sql, 'WHERE `colA` = "x"')

    def test_first_call_not_include_empty(self):
        gen = _make_generator('`colA` = "x"')
        gen._update_filter_sql(False, self.col_b)
        self.assertEqual(gen.filter_sql, 'WHERE `colB` is not null AND (`colA` = "x")')

    def test_repeated_call_include_empty(self):
        gen = _make_generator('WHERE `colA` = "x"')
        gen._update_filter_sql(True, self.col_a)
        self.assertEqual(gen.filter_sql, 'WHERE `colA` = "x"')

    def test_repeated_call_not_include_empty(self):
        gen = _make_generator('WHERE `colA` = "x"')
        gen._update_filter_sql(False, self.col_b)
        self.assertEqual(gen.filter_sql, 'WHERE `colB` is not null AND (`colA` = "x")')

    def test_repeated_call_different_column(self):
        gen = _make_generator('WHERE `colA` is not null AND (`colB` = 1)')
        gen._update_filter_sql(False, self.col_a)
        self.assertEqual(gen.filter_sql, 'WHERE `colA` is not null AND (`colA` is not null AND (`colB` = 1))')

    def test_idempotent_include_empty(self):
        gen = _make_generator('`colA` = "x"')
        gen._update_filter_sql(True, self.col_a)
        gen._update_filter_sql(True, self.col_a)
        self.assertEqual(gen.filter_sql, 'WHERE `colA` = "x"')

    def test_no_duplicate_where_prefix(self):
        gen = _make_generator('`colA` = "x"')
        gen._update_filter_sql(False, self.col_b)
        gen._update_filter_sql(False, self.col_b)
        self.assertFalse(gen.filter_sql.startswith('WHERE WHERE'))


if __name__ == '__main__':
    unittest.main()
