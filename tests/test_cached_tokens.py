"""Exercise statistics without initializing deployment config, Redis or MySQL."""
import ast
from collections import defaultdict
from copy import deepcopy
from datetime import datetime
import logging
from pathlib import Path
import unittest
from unittest.mock import MagicMock


SOURCE = Path(__file__).resolve().parents[1] / 'dtable_events'


def load_definitions(path, names, namespace):
    tree = ast.parse(path.read_text())
    tree.body = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    exec(compile(tree, str(path), 'exec'), namespace)
    return namespace


class CachedTokensTest(unittest.TestCase):
    def setUp(self):
        self.prices = {'test-model': {'input_tokens': 2, 'cached_tokens': 0.5, 'output_tokens': 4}}
        self.namespace = load_definitions(SOURCE / 'tasks/ai_stats_worker.py',
                                          {'AIStatsWorker', 'calculate_usage_cost'}, {
            'AI_PRICES': self.prices,
            'defaultdict': defaultdict,
            'deepcopy': deepcopy,
            'datetime': datetime,
            'logger': logging.getLogger(__name__),
            'text': lambda sql: sql,
            'uuid_str_to_32_chars': lambda uuid: uuid.replace('-', ''),
        })
        worker_class = self.namespace['AIStatsWorker']
        self.worker = worker_class.__new__(worker_class)
        self.worker.reset_stats()
        self.worker._get_dtable_owner_info = MagicMock(return_value={'owner': 'owner', 'org_id': 1})

    def save(self, cached_tokens=None, canonical=False):
        usage = {'input_tokens': 1000, 'output_tokens': 100} if canonical else {'prompt_tokens': 1000, 'completion_tokens': 100}
        if cached_tokens is not None:
            usage['cached_tokens'] = cached_tokens
        self.worker.save_to_memory({
            'model': 'test-model', 'dtable_uuid': 'base', 'usage': usage,
            'scenario': 'app_page_generation',
        }, MagicMock())

    def test_normalize_and_aggregate(self):
        for value, expected in [(None, 0), (0, 0), (400, 400), (1000, 1000), (-10, 0), (2000, 1000), ('bad', 0)]:
            with self.subTest(cached_tokens=value):
                self.worker.reset_stats()
                self.save(value)
                self.save(value, canonical=True)
                expected_usage = {'input_tokens': 2000, 'cached_tokens': expected * 2, 'output_tokens': 200}
                self.assertEqual(self.worker.org_stats[1]['test-model'], expected_usage)
                self.assertEqual(self.worker.owner_stats['owner']['test-model'], expected_usage)
                self.assertEqual(self.worker.dtable_stats['base'][('test-model', 'app_page_generation')], expected_usage)

    def test_cost_and_default_price(self):
        calculate = self.namespace['calculate_usage_cost']
        for cached, expected in [(0, 0.0024), (400, 0.0018), (1000, 0.0009)]:
            self.assertAlmostEqual(calculate('test-model', 1000, cached, 100), expected)
        self.prices['test-model']['cached_tokens'] = 0
        self.assertAlmostEqual(calculate('test-model', 1000, 400, 100), 0.0016)
        del self.prices['test-model']['cached_tokens']
        self.assertAlmostEqual(calculate('test-model', 1000, 400, 100), 0.0024)

    def test_flush_all_three_statistics_tables(self):
        self.save(400)
        self.worker.stats_lock = MagicMock()
        self.worker.query_dtable_owners = MagicMock(return_value={'base': {'owner': 'owner', 'org_id': 1}})
        session = MagicMock()
        self.worker._db_session_class = MagicMock(return_value=session)
        self.worker.stats_worker()
        self.assertEqual(session.execute.call_count, 3)
        for call in session.execute.call_args_list:
            sql, rows = call.args
            self.assertIn('`cached_tokens`', sql)
            self.assertEqual(rows[0]['cached_tokens'], 400)
            self.assertAlmostEqual(rows[0]['cost'], 0.0018)
        session.commit.assert_called_once()

    def test_price_configuration(self):
        parse = load_definitions(SOURCE / 'app/config.py', {'get_llm_prices'}, {})['get_llm_prices']
        for price, expected in [(2, 2), ({'input_tokens': 2}, 2), ({'input_tokens': 2, 'cached_tokens': 0}, 0),
                                ({'input_tokens': 2, 'cached_tokens': 0.5}, 0.5)]:
            with self.subTest(price=price):
                self.assertEqual(parse([{'model': 'test-model', 'price': price}])['test-model']['cached_tokens'], expected)


if __name__ == '__main__':
    unittest.main()
