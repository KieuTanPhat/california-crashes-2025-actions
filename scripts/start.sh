#!/usr/bin/env bash
set -euo pipefail
docker compose up -d > .runtime/logs/compose-up.log 2>&1
"$SPARK_HOME/sbin/start-history-server.sh" > .runtime/logs/history-start.log 2>&1
.venv-airflow/bin/airflow db migrate > .runtime/logs/airflow-db.log 2>&1
LAB_UI_PASSWORD="$(python -c 'import secrets; print(secrets.token_urlsafe(24))')"
echo "::add-mask::$LAB_UI_PASSWORD"
printf '%s' "$LAB_UI_PASSWORD" > .runtime/ui-password
chmod 600 .runtime/ui-password
.venv-airflow/bin/airflow users create --username report --password "$LAB_UI_PASSWORD" \
 --firstname Lab --lastname Report --role Admin --email lab@example.invalid > .runtime/logs/airflow-user.log 2>&1
nohup .venv-airflow/bin/airflow scheduler > .runtime/logs/airflow-scheduler.log 2>&1 &
nohup .venv-airflow/bin/airflow webserver --port 8080 > .runtime/logs/airflow-webserver.log 2>&1 &
.venv-data/bin/python - <<'PY'
import time, requests
from kafka import KafkaAdminClient
from pathlib import Path
password=Path('.runtime/ui-password').read_text()
for attempt in range(90):
    try:
        client=KafkaAdminClient(bootstrap_servers='127.0.0.1:9092',request_timeout_ms=5000)
        client.close()
        r=requests.get('http://127.0.0.1:8080/api/v1/dags/california_crashes_2025',auth=('report',password),timeout=5)
        r.raise_for_status()
        if not r.json().get('is_paused',True):
            print('Kafka and Airflow DAG are ready')
            break
    except Exception:
        pass
    time.sleep(3)
else:
    raise SystemExit('Kafka/Airflow did not become ready within 270 seconds')
PY
