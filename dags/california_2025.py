"""The actual scheduler runs these tasks; no fabricated DAG execution."""
import os
import shlex
from datetime import datetime, timedelta, timezone
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator

root = Path(os.environ['PROJECT_ROOT'])
names = ['prepare_hdfs', 'upload_csv', 'spark_read', 'clean_parquet', 'spark_sql',
         'rfm_kmeans', 'save_hdfs', 'kafka_ingest', 'verify_outputs']
with DAG('california_crashes_2025', start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
         schedule=None, catchup=False, max_active_runs=1,
         default_args={'owner': 'california-project', 'retries': 1, 'retry_delay': timedelta(seconds=10)},
         tags=['California', '2025', 'HDFS', 'Spark', 'Kafka', 'RFM-adjusted']) as dag:
    previous = None
    for i, name in enumerate(names, 1):
        command = f'cd {shlex.quote(str(root))} && {shlex.quote(str(root / ".venv-data/bin/python"))} pipeline/stages.py {i}'
        task = BashOperator(task_id=f'step_{i:02d}_{name}', bash_command=command,
                            execution_timeout=timedelta(minutes=15), do_xcom_push=False)
        if previous:
            previous >> task
        previous = task
