import os

import pendulum
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import DAG
except ImportError:
    from airflow.models.dag import DAG


def dag_failure_callback(context):
    """DAG-level callback that gets executed when the DAG fails.

    This callback is executed by the DAG Processor via the callback fetching mechanism.
    """
    os.makedirs("/tmp/task_callback", exist_ok=True)
    with open("/tmp/task_callback/dag_failure.txt", "w") as f:
        f.write("DAG failed and callback was executed by DAG Processor")


with DAG(
    "task_callback_dag",
    schedule=None,
    start_date=pendulum.datetime(2024, 12, 1, tz="UTC"),
    on_failure_callback=dag_failure_callback,
):
    # Task that will fail to trigger the DAG failure callback
    BashOperator(
        task_id="failing_task",
        bash_command="exit 1",  # This will fail
        cwd=".",
    )
