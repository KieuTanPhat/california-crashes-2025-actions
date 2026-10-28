"""Capture a live ttyd/xterm terminal attached to a real Bash PTY on the runner.

These commands verify the real state after the Airflow DAG. Output is not
replayed from logs, rendered into fake terminal HTML, or generated as an image.
"""
import datetime as dt
import html
import json
import os
from pathlib import Path
import shlex
import subprocess
import time
import requests
from playwright.sync_api import sync_playwright
from terminal_contract import read_exit_status, require_success

ROOT = Path(os.environ['PROJECT_ROOT'])
OUT = ROOT / 'artifacts/evidence/terminal'
RUNTIME = ROOT / '.runtime/terminal'
OUT.mkdir(parents=True, exist_ok=True)
RUNTIME.mkdir(parents=True, exist_ok=True)
marker = RUNTIME / 'last-exit'
rcfile = RUNTIME / 'bashrc'
rcfile.write_text('''export PS1='\\[\\e[32m\\]\\u@\\h\\[\\e[0m\\]:\\[\\e[34m\\]\\W\\[\\e[0m\\]\\$ '
export TERM=xterm-256color
export LANG=C.UTF-8
set -o pipefail
bind 'set enable-bracketed-paste off'
PROMPT_COMMAND='__lab_status=$?; printf "%s\\n" "$__lab_status" > "$LAB_EXIT_MARKER"'
''')
env = dict(os.environ, LAB_EXIT_MARKER=str(marker))
help_text = subprocess.run(['ttyd', '--help'], capture_output=True, text=True).stdout
args = ['ttyd', '-i', '127.0.0.1', '-p', '7681', '-t', 'fontSize=18', '-t', 'scrollback=3000']
if '--writable' in help_text:
    args.append('--writable')
shell_command = shlex.join(['bash', '--rcfile', str(rcfile), '-i'])
args += ['script', '-q', '-f', '-c', shell_command, str(OUT / 'session.typescript')]
log = (ROOT / '.runtime/logs/ttyd.log').open('w')
server = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
spark = '"$SPARK_HOME/bin/spark-submit" --master local[2] --conf spark.ui.showConsoleProgress=false pipeline/terminal_checks.py '
def spark_cmd(mode):
    return spark + mode + ' 2> artifacts/evidence/terminal/' + mode + '-stderr.log'

plan = [
    (0, '00-environment.png', 'Môi trường GitHub Actions', [
        'printf "GitHub Actions run: %s | mode: %s\\n" "$GITHUB_RUN_ID" "$LAB_MODE"',
        'hostname', 'pwd', 'java -version',
        '.venv-data/bin/python -c "import pyspark; print(\'PySpark version:\', pyspark.__version__)"']),
    (1, '01-hdfs-commands.png', 'HDFS: tạo thư mục và kiểm tra DataNode', [
        '"$HADOOP_HOME/bin/hdfs" dfs -mkdir -p /california2025/raw /california2025/processed /california2025/output',
        '"$HADOOP_HOME/bin/hdfs" dfs -ls /california2025',
        '"$HADOOP_HOME/bin/hdfs" dfsadmin -report']),
    (2, '02-upload-commands.png', 'Upload lại cùng CSV và kiểm tra checksum', [
        '"$HADOOP_HOME/bin/hdfs" dfs -put -f data/input.csv /california2025/raw/input.csv',
        '"$HADOOP_HOME/bin/hdfs" dfs -ls /california2025/raw',
        'sha256sum data/input.csv',
        '"$HADOOP_HOME/bin/hdfs" dfs -cat /california2025/raw/input.csv | sha256sum']),
    (3, '03-spark-read-command.png', 'Spark đọc trực tiếp CSV từ HDFS', [spark_cmd('read')]),
    (4, '04-clean-parquet-command.png', 'Đọc Parquet đã làm sạch và kiểm tra NULL', [spark_cmd('clean')]),
    (5, '05-spark-sql-command.png', 'Spark SQL thực thi trên Parquet HDFS', [spark_cmd('sql')]),
    (6, '06-rfm-kmeans-command.png', 'Nạp model thật từ HDFS và xem các cụm', [spark_cmd('rfm')]),
    (7, '07-save-hdfs-commands.png', 'Kiểm tra dữ liệu và model đã lưu HDFS', [
        '"$HADOOP_HOME/bin/hdfs" dfs -ls /california2025/processed/cleaned',
        '"$HADOOP_HOME/bin/hdfs" dfs -ls /california2025/output/tables',
        '"$HADOOP_HOME/bin/hdfs" dfs -ls /california2025/output/models']),
    (8, '08-kafka-commands.png', 'Kafka CLI và dữ liệu Structured Streaming thật', [
        'TOPIC=$(.venv-data/bin/python -c "import json; print(json.load(open(\'artifacts/data-quality/stream-reconciliation.json\'))[\'topic\'])")',
        'docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$TOPIC"',
        'docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC"',
        'docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" --from-beginning --max-messages 3 --timeout-ms 10000',
        spark_cmd('stream')]),
    (9, '09-airflow-commands.png', 'Airflow CLI: trạng thái DAG và 9 task', [
        '.venv-airflow/bin/airflow dags list-runs -d california_crashes_2025 -o table 2> artifacts/evidence/terminal/airflow-stderr.log',
        '.venv-airflow/bin/airflow tasks states-for-dag-run california_crashes_2025 "$LAB_RUN_KEY" -o table 2>> artifacts/evidence/terminal/airflow-stderr.log']),
]

results = []
try:
    for attempt in range(60):
        try:
            if requests.get('http://127.0.0.1:7681/', timeout=2).ok:
                break
        except requests.RequestException:
            pass
        if server.poll() is not None:
            raise RuntimeError('ttyd exited before starting; see ttyd.log')
        time.sleep(.5)
    else:
        raise RuntimeError('ttyd did not become ready')
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page(viewport={'width': 1800, 'height': 1350}, device_scale_factor=1)
        page.goto('http://127.0.0.1:7681/', wait_until='domcontentloaded')
        page.locator('.xterm-helper-textarea').wait_for(state='attached', timeout=30000)
        deadline = time.monotonic() + 30
        while read_exit_status(marker) is None:
            if time.monotonic() > deadline:
                raise RuntimeError('Live Bash prompt did not initialize')
            page.wait_for_timeout(100)

        def run_command(command):
            marker.unlink(missing_ok=True)
            page.locator('.xterm-helper-textarea').focus()
            page.keyboard.type(command, delay=1)
            page.keyboard.press('Enter')
            until = time.monotonic() + 300
            status = None
            while status is None:
                if time.monotonic() > until:
                    page.screenshot(path=str(OUT / 'terminal-timeout.png'))
                    raise TimeoutError('Terminal command did not complete: ' + command)
                page.wait_for_timeout(100)
                status = read_exit_status(marker)
            if status:
                page.screenshot(path=str(OUT / 'terminal-command-failed.png'))
            require_success(status)
            return {'command': command, 'exit_code': status, 'verified_at_utc': dt.datetime.now(dt.timezone.utc).isoformat()}

        for step, filename, title, commands in plan:
            run_command('clear')
            records = []
            for command in commands:
                records.append(run_command(command))
            page.wait_for_timeout(300)
            page.screenshot(path=str(OUT / filename))
            results.append({'step': step, 'path': filename, 'title': title, 'commands': records,
                            'kind': 'Live ttyd/xterm Bash PTY screenshot', 'run_id': os.environ['GITHUB_RUN_ID']})
            (OUT / 'terminal-index.json').write_text(json.dumps(results, ensure_ascii=False, indent=2))
            print(f'Terminal screenshot {filename}: {len(records)} actual commands, all exit=0', flush=True)
        browser.close()
finally:
    server.terminate()
    try:
        server.wait(timeout=10)
    except subprocess.TimeoutExpired:
        server.kill()
    log.close()

sections = ''.join('<section><h2>' + html.escape(r['title']) + '</h2><img src="' + r['path'] + '"><pre>' + html.escape('\n'.join(c['command'] for c in r['commands'])) + '</pre></section>' for r in results)
(OUT / 'index.html').write_text('''<!doctype html><meta charset="utf-8"><title>Ảnh terminal thật trên GitHub Actions</title>
<style>body{font-family:Arial;background:#eef1f6;margin:30px}section{background:white;padding:24px;margin:24px 0}img{width:100%;max-width:1800px}pre{white-space:pre-wrap}</style>
<h1>Ảnh lệnh thực thi trong terminal Bash thật</h1><p>Các lệnh kiểm chứng chạy sau DAG, trên cùng runner và dữ liệu HDFS/Kafka/Airflow đang hoạt động. Có transcript PTY và exit code cho từng lệnh. Đây không phải ảnh dựng lại từ log.</p>''' + sections, encoding='utf-8')
if len(results) != 10:
    raise RuntimeError('Terminal evidence is incomplete')
