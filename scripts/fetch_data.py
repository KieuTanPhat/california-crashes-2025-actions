import csv
import hashlib
import json
import os
import urllib.request
from pathlib import Path

cfg = json.loads(Path('config/data.json').read_text())
source = Path('data/original.csv')
with urllib.request.urlopen(cfg['url'], timeout=120) as remote, source.open('wb') as output:
    while chunk := remote.read(1024 * 1024):
        output.write(chunk)
actual = hashlib.file_digest(source.open('rb'), 'sha256').hexdigest()
if actual != cfg['sha256'] or source.stat().st_size != cfg['bytes']:
    raise SystemExit('CSV checksum/size mismatch: refusing to process a different snapshot')
mode = os.environ.get('LAB_MODE', 'smoke')
if mode not in ('smoke', 'full'):
    raise SystemExit('Unknown mode')
count = 0
selected = 0
with source.open(encoding='utf-8-sig', newline='') as f, Path('data/input.csv').open('w', encoding='utf-8', newline='') as out:
    reader = csv.reader(f)
    writer = csv.writer(out)
    header = next(reader)
    if len(header) != cfg['columns']:
        raise SystemExit('Unexpected number of CSV columns')
    writer.writerow(header)
    for row in reader:
        count += 1
        if len(row) != len(header):
            raise SystemExit(f'Malformed CSV record at row {count}')
        if mode == 'full' or selected < 5000:
            writer.writerow(row)
            selected += 1
if count != cfg['rows']:
    raise SystemExit('Original CSV row count differs from frozen metadata')
meta = dict(mode=mode, source_rows=count, selected_rows=selected, columns=len(header),
            original_sha256=actual, input_sha256=hashlib.file_digest(Path('data/input.csv').open('rb'), 'sha256').hexdigest(),
            source_bytes=source.stat().st_size, run_key=os.environ['LAB_RUN_KEY'])
Path('.runtime/input.json').write_text(json.dumps(meta, indent=2))
Path('artifacts/manifest/input.json').write_text(json.dumps(meta, indent=2))
print(json.dumps(meta))
