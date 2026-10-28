#!/usr/bin/env bash
set -euo pipefail
mkdir -p .runtime/logs artifacts/{evidence,results,data-quality,manifest} data
export PROJECT_ROOT="$PWD"
python -m venv .venv-data
.venv-data/bin/pip install --disable-pip-version-check -q -r requirements-data.txt
python -m venv .venv-airflow
.venv-airflow/bin/pip install --disable-pip-version-check -q 'apache-airflow==2.10.5' \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt
.venv-data/bin/python -m playwright install --with-deps chromium

if [[ ! -d .runtime/hadoop ]]; then
  curl --fail --location --retry 3 --output .runtime/hadoop.tar.gz \
    https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
  curl --fail --location --retry 3 --output .runtime/hadoop.sha512 \
    https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz.sha512
  python - <<'PY'
from pathlib import Path
import hashlib, re
p=Path('.runtime/hadoop.tar.gz')
actual=hashlib.file_digest(p.open('rb'),'sha512').hexdigest()
expected=re.findall(r'\b[0-9a-fA-F]{128}\b',Path('.runtime/hadoop.sha512').read_text())
if not expected or actual.lower()!=expected[0].lower():
    raise SystemExit('Hadoop download SHA512 mismatch')
print('Hadoop SHA512 verified')
PY
  mkdir -p .runtime/hadoop
  tar -xzf .runtime/hadoop.tar.gz -C .runtime/hadoop --strip-components=1
  rm .runtime/hadoop.tar.gz
fi
export HADOOP_HOME="$PWD/.runtime/hadoop"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME
SPARK_HOME="$(.venv-data/bin/python -c 'import os,pyspark; print(os.path.dirname(pyspark.__file__))')"
export AIRFLOW_HOME="$PWD/.runtime/airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=False
export AIRFLOW__CORE__PARALLELISM=1
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=5
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=5
export AIRFLOW__SCHEDULER__PARSING_PROCESSES=1
export AIRFLOW__WEBSERVER__WORKERS=1
export AIRFLOW__WEBSERVER__WEB_SERVER_HOST=127.0.0.1
export AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv-data/bin/python"
export PYSPARK_SUBMIT_ARGS='--driver-memory 2g pyspark-shell'
mkdir -p "$AIRFLOW_HOME" .runtime/spark-events
cat > "$HADOOP_CONF_DIR/core-site.xml" <<EOF
<configuration><property><name>fs.defaultFS</name><value>hdfs://127.0.0.1:9000</value></property></configuration>
EOF
cat > "$HADOOP_CONF_DIR/hdfs-site.xml" <<EOF
<configuration>
<property><name>dfs.replication</name><value>1</value></property>
<property><name>dfs.namenode.name.dir</name><value>file://$PWD/.runtime/hdfs/name</value></property>
<property><name>dfs.datanode.data.dir</name><value>file://$PWD/.runtime/hdfs/data</value></property>
<property><name>dfs.namenode.rpc-address</name><value>127.0.0.1:9000</value></property>
<property><name>dfs.namenode.http-address</name><value>127.0.0.1:9870</value></property>
<property><name>dfs.datanode.http.address</name><value>127.0.0.1:9864</value></property>
<property><name>dfs.datanode.address</name><value>127.0.0.1:9866</value></property>
<property><name>dfs.datanode.ipc.address</name><value>127.0.0.1:9867</value></property>
<property><name>dfs.datanode.hostname</name><value>127.0.0.1</value></property>
<property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
<property><name>dfs.permissions.enabled</name><value>false</value></property>
<property><name>dfs.webhdfs.enabled</name><value>true</value></property>
</configuration>
EOF
echo "export JAVA_HOME=$JAVA_HOME" >> "$HADOOP_CONF_DIR/hadoop-env.sh"
echo 'export HADOOP_HEAPSIZE_MAX=256' >> "$HADOOP_CONF_DIR/hadoop-env.sh"
mkdir -p .runtime/spark-conf
cat > .runtime/spark-conf/spark-defaults.conf <<EOF
spark.history.fs.logDirectory file://$PWD/.runtime/spark-events
spark.history.ui.port 18080
spark.history.ui.bindAddress 127.0.0.1
spark.history.fs.update.interval 2s
EOF
export SPARK_CONF_DIR="$PWD/.runtime/spark-conf"
export SPARK_DAEMON_MEMORY=512m
# Export only named runtime configuration. No credential is written to GITHUB_ENV.
for var in PROJECT_ROOT HADOOP_HOME HADOOP_CONF_DIR SPARK_HOME SPARK_CONF_DIR SPARK_DAEMON_MEMORY AIRFLOW_HOME \
 AIRFLOW__CORE__DAGS_FOLDER AIRFLOW__CORE__LOAD_EXAMPLES AIRFLOW__CORE__EXECUTOR \
 AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION AIRFLOW__CORE__PARALLELISM \
 AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL \
 AIRFLOW__SCHEDULER__PARSING_PROCESSES AIRFLOW__WEBSERVER__WORKERS \
 AIRFLOW__WEBSERVER__WEB_SERVER_HOST AIRFLOW__API__AUTH_BACKENDS SPARK_LOCAL_IP PYSPARK_PYTHON PYSPARK_SUBMIT_ARGS; do
  echo "$var=${!var}" >> "$GITHUB_ENV"
done
.venv-data/bin/pip freeze > artifacts/manifest/python-data-lock.txt
.venv-airflow/bin/pip freeze > artifacts/manifest/python-airflow-lock.txt
java -version > artifacts/manifest/java-version.txt 2>&1
docker compose version > artifacts/manifest/compose-version.txt
