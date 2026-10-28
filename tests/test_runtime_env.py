import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

class RuntimeEnvironmentTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('bash'), 'Runner test needs bash')
    def test_scheduler_children_can_find_airflow_cli(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / '.venv-airflow/bin/airflow'
            binary.parent.mkdir(parents=True)
            binary.write_text('#!/bin/sh\necho lab-airflow-ok\n')
            binary.chmod(0o755)
            result = subprocess.run(['bash', '-c', 'source "$1" && airflow', 'test', str(ROOT / 'scripts/runtime_env.sh')],
                                    env=dict(os.environ, PROJECT_ROOT=str(root)), capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), 'lab-airflow-ok')
