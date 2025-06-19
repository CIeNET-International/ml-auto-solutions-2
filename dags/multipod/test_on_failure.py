import logging

from airflow import models
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowException


"""
1. add 'apache-airflow-providers-github' at composer/Pypi packages/Edit
2. Add 'Personal Access Token' into Airflow/Connections
2.5. Use 'github_default' as conn_id, choose Github as type and fill in token
3. Append this operator(task) at the end of target DAGs
"""


@task
def task_a():
  logging.info("task A")


@task
def task_b():
  logging.info("task B")
  raise AirflowException("task B failed")


@task
def task_c():
  logging.info("task C")


with models.DAG(
    dag_id="test_on_failure",
    schedule=None,
    tags=[
      "test_dag",
      "github_enabled",
    ],
    catchup=False,
    default_args={
        'retries': 0,  # ← 這裡直接指定 Task 重試次數
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
  taskA = task_a()
  taskB = task_b()
  taskC = task_c()
  taskA >> taskB >> taskC

