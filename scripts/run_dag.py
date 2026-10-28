"""Trigger and monitor a scheduler-managed Airflow DagRun through its real API."""
import json
import os
import time
from pathlib import Path
import requests

base = 'http://127.0.0.1:8080/api/v1/dags/california_crashes_2025'
session = requests.Session()
session.auth = ('report', Path('.runtime/ui-password').read_text())
run_id = os.environ['LAB_RUN_KEY']
response = session.post(base + '/dagRuns', json={'dag_run_id': run_id}, timeout=30)
response.raise_for_status()
previous = None
for attempt in range(600):
    r = session.get(base + '/dagRuns/' + run_id, timeout=30)
    r.raise_for_status()
    state = r.json()['state']
    tasks = session.get(base + '/dagRuns/' + run_id + '/taskInstances', timeout=30)
    tasks.raise_for_status()
    task_data = tasks.json()['task_instances']
    states = {t['task_id']: t['state'] for t in task_data}
    if states != previous:
        print(json.dumps({'dag_state': state, 'tasks': states}), flush=True)
        previous = states
    if state in ('success', 'failed'):
        Path('artifacts/manifest/airflow-run.json').write_text(json.dumps(r.json(), indent=2))
        Path('artifacts/manifest/airflow-tasks.json').write_text(json.dumps(task_data, indent=2))
        if state != 'success' or len(task_data) != 9 or any(t['state'] != 'success' for t in task_data):
            raise SystemExit('Airflow DAG or one of its nine tasks failed; see task logs')
        print('Verified: real scheduler DagRun success, 9/9 tasks success')
        break
    time.sleep(5)
else:
    raise SystemExit('DAG exceeded the 50 minute monitoring timeout')
