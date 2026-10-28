#!/usr/bin/env bash
# SequentialExecutor invokes `airflow tasks run` by executable name.
# The scheduler and all of its children must inherit this environment.
export PATH="$PROJECT_ROOT/.venv-airflow/bin:$PATH"
