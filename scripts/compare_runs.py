import json
import hashlib
from pathlib import Path

root = Path('.runtime/proof')
statuses = [json.loads((root / f'{key}-status.json').read_text()) for key in ('first', 'second')]
if any(s['conclusion'] != 'success' for s in statuses):
    raise SystemExit('Both workflow runs must have conclusion=success')
manifests = []
download_checks = []
for folder in ('first', 'second'):
    files = list((root / folder).glob('*/manifest/run.json'))
    if len(files) != 1:
        raise SystemExit(f'Expected exactly one manifest for {folder}')
    manifest = json.loads(files[0].read_text())
    manifests.append(manifest)
    artifact = files[0].parent.parent
    valid = True
    for name, expected_hash in manifest['result_sha256'].items():
        file = artifact / 'results' / name
        if Path(name).name != name or not file.is_file():
            valid = False
            continue
        with file.open('rb') as stream:
            actual_hash = hashlib.file_digest(stream, 'sha256').hexdigest()
        valid = valid and actual_hash == expected_hash
    download_checks.append(valid and bool(manifest['result_sha256']))
a, b = manifests
checks = {
    'both_full': a['mode'] == b['mode'] == 'full',
    'same_commit': a['commit'] == b['commit'] == statuses[0]['headSha'] == statuses[1]['headSha'],
    'same_original_snapshot': a['input']['original_sha256'] == b['input']['original_sha256'],
    'same_full_input': a['input']['input_sha256'] == b['input']['input_sha256'],
    'full_source_processed': a['quality']['input_rows'] == b['quality']['input_rows'] == 402354,
    'same_cleaning_statistics': a['quality'] == b['quality'],
    'same_result_signatures': a['result_sha256'] == b['result_sha256'],
    'downloaded_results_match_recorded_hashes': all(download_checks),
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
