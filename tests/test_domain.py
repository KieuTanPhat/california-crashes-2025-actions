"""Data contracts: deliberately use tiny fixtures, never the private local machine."""
import importlib.util
import unittest
from pathlib import Path

class DomainTests(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location('domain', Path('pipeline/domain.py'))
        self.assertTrue(Path('pipeline/domain.py').is_file(), 'Missing production data contracts')
        self.domain = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.domain)

    def test_header_trims_tabs_and_spaces(self):
        self.assertEqual(self.domain.normalize_name('\tNumberKilled '), 'number_killed')
        self.assertEqual(self.domain.normalize_name('Crash Date Time'), 'crash_date_time')
        self.assertEqual(self.domain.normalize_name('NumberInjured'), 'number_injured')

    def test_unknown_casualties_are_not_zero(self):
        self.assertIsNone(self.domain.casualty_count(''))
        self.assertIsNone(self.domain.casualty_count('-1'))
        self.assertIsNone(self.domain.casualty_count('bad'))
        self.assertIsNone(self.domain.casualty_count('2.5'))
        self.assertEqual(self.domain.casualty_count('0'), 0)
        self.assertEqual(self.domain.casualty_count(' 3 '), 3)

    def test_recency_uses_fixed_year_end(self):
        self.assertEqual(self.domain.recency_days('2025-12-31'), 0)
        self.assertEqual(self.domain.recency_days('2025-01-01'), 364)
        with self.assertRaises(ValueError):
            self.domain.recency_days('2026-01-01')

    def test_m_is_observed_casualties_and_unknown_stays_unknown(self):
        self.assertEqual(self.domain.severity_m(2, 1), 3)
        self.assertIsNone(self.domain.severity_m(None, 1))
        self.assertIsNone(self.domain.severity_m(2, None))

    def test_reconciliation_detects_lost_rows(self):
        self.domain.reconcile_rows(10, 7, 2, 1)
        with self.assertRaises(ValueError):
            self.domain.reconcile_rows(10, 7, 1, 1)

    def test_stream_identity_includes_run_and_collision(self):
        self.assertEqual(self.domain.event_key('run42', '101'), 'run42:101')
        self.assertNotEqual(self.domain.event_key('a', '101'), self.domain.event_key('b', '101'))

    def test_coordinates_require_california_envelope(self):
        self.assertTrue(self.domain.valid_california_point(34, -118))
        for point in [(0, 0), (None, -118), (100, -118), (40, 120)]:
            self.assertFalse(self.domain.valid_california_point(*point))
