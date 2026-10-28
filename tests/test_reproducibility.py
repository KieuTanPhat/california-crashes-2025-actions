import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'scripts/compare_runs.py'

class ReproducibilityTests(unittest.TestCase):
    def compare(self, mode):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'artifacts/proof').mkdir(parents=True)
            expected = b'month,crashes\n1,42\n'
            manifest = {'mode': 'full', 'commit': 'abc123',
                        'input': {'original_sha256': 'original', 'input_sha256': 'fullinput'},
                        'quality': {'input_rows': 402354},
                        'result_sha256': {'monthly.csv': hashlib.sha256(expected).hexdigest()},
                        'stream': {'event_ids_exact_match': True}}
            for key in ('first', 'second'):
                folder = root / '.runtime/proof' / key / 'artifact'
                (folder / 'manifest').mkdir(parents=True)
                (folder / 'results').mkdir()
                (folder / 'manifest/run.json').write_text(json.dumps(manifest))
                (folder / 'results/monthly.csv').write_bytes(expected)
                (root / '.runtime/proof' / f'{key}-status.json').write_text(json.dumps({'conclusion': 'success', 'headSha': 'abc123', 'url': key}))
            target = root / '.runtime/proof/second/artifact/results/monthly.csv'
            if mode == 'tampered':
                target.write_bytes(b'month,crashes\n1,999\n')
            elif mode == 'missing':
                target.unlink()
            env = dict(os.environ, GITHUB_STEP_SUMMARY=str(root / 'summary.md'))
            return subprocess.run([sys.executable, str(SCRIPT)], cwd=root, env=env, text=True, capture_output=True)

    def test_intact_downloads_pass(self):
        result = self.compare('intact')
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_tampered_download_cannot_pass(self):
        self.assertNotEqual(self.compare('tampered').returncode, 0, 'Modified CSV passed reproducibility')

    def test_missing_download_cannot_pass(self):
        self.assertNotEqual(self.compare('missing').returncode, 0, 'Missing CSV passed reproducibility')
