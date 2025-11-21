"""
Example DAG demonstrating ``TimeDeltaSensorAsync``, a drop in replacement for ``TimeDeltaSensor`` that
defers and doesn't occupy a worker slot while it waits
"""

from __future__ import annotations

import datetime

import pendulum

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensorAsync

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG

with DAG(
    dag_id="deferrable",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
) as dag:
    wait = TimeDeltaSensorAsync(task_id="wait", delta=datetime.timedelta(seconds=10))
    finish = EmptyOperator(task_id="finish")
    wait >> finish
