import json
from pathlib import Path

root = Path('.runtime/proof')
statuses = [json.loads((root / f'{key}-status.json').read_text()) for key in ('first', 'second')]
if any(s['conclusion'] != 'success' for s in statuses):
    raise SystemExit('Both workflow runs must have conclusion=success')
manifests = []
for folder in ('first', 'second'):
    files = list((root / folder).glob('*/manifest/run.json'))
    if len(files) != 1:
        raise SystemExit(f'Expected exactly one manifest for {folder}')
    manifests.append(json.loads(files[0].read_text()))
a, b = manifests
checks = {
    'both_full': a['mode'] == b['mode'] == 'full',
    'same_commit': a['commit'] == b['commit'] == statuses[0]['headSha'] == statuses[1]['headSha'],
    'same_original_snapshot': a['input']['original_sha256'] == b['input']['original_sha256'],
    'same_full_input': a['input']['input_sha256'] == b['input']['input_sha256'],
    'full_source_processed': a['quality']['input_rows'] == b['quality']['input_rows'] == 402354,
    'same_cleaning_statistics': a['quality'] == b['quality'],
    'same_result_signatures': a['result_sha256'] == b['result_sha256'],
    'both_stream_reconciliations_pass': a['stream']['event_ids_exact_match'] and b['stream']['event_ids_exact_match'],
}
proof = {'status': 'PASS' if all(checks.values()) else 'FAIL', 'checks': checks,
         'runs': [s['url'] for s in statuses], 'commit': a['commit'],
         'original_sha256': a['input']['original_sha256'], 'result_sha256': a['result_sha256']}
Path('artifacts/proof/comparison.json').write_text(json.dumps(proof, indent=2))
summary = '# Reproducibility: ' + proof['status'] + '\n\n' + '\n'.join(f'- {k}: {"PASS" if v else "FAIL"}' for k, v in checks.items()) + '\n'
Path('artifacts/proof/comparison.md').write_text(summary)
print(summary)
import os
with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as f:
    f.write(summary)
if not all(checks.values()):
    raise SystemExit('Two full runs are not reproducible under the declared contract')
