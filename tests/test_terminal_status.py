import importlib.util
from pathlib import Path
import tempfile
import unittest

class TerminalStatusTests(unittest.TestCase):
    def load(self):
        path = Path('scripts/terminal_contract.py')
        self.assertTrue(path.is_file(), 'Missing real terminal exit-status contract')
        spec = importlib.util.spec_from_file_location('terminal_contract', path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_missing_prompt_marker_is_not_success(self):
        module = self.load()
        with tempfile.TemporaryDirectory() as temp:
            self.assertIsNone(module.read_exit_status(Path(temp) / 'not-yet-finished'))

    def test_real_nonzero_exit_is_rejected(self):
        module = self.load()
        with tempfile.TemporaryDirectory() as temp:
            marker = Path(temp) / 'status'
            marker.write_text('7\n')
            with self.assertRaises(RuntimeError):
                module.require_success(module.read_exit_status(marker))

    def test_zero_exit_is_accepted(self):
        module = self.load()
        with tempfile.TemporaryDirectory() as temp:
            marker = Path(temp) / 'status'
            marker.write_text('0\n')
            self.assertEqual(module.read_exit_status(marker), 0)
            module.require_success(0)

    def test_corrupt_marker_is_not_success(self):
        module = self.load()
        with tempfile.TemporaryDirectory() as temp:
            marker = Path(temp) / 'status'
            marker.write_text('not an exit code')
            with self.assertRaises(ValueError):
                module.read_exit_status(marker)
