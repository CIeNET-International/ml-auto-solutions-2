import logging

from airflow import models
from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context
from airflow.providers.github.operators.github import GithubOperator
from github import Repository

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


def create_issue(repo: Repository):
  context = get_current_context()
  dag_run = context['dag_run']
  failed_tis = dag_run.get_task_instances(state='failed')
  error_msg = ""
  for ti in failed_tis:
      # Check Issue body format
      logging.error(f"DAG: {ti.dag_id}, Run: {ti.run_id}, Task: {ti.task_id} failed")
      error_msg += (f"DAG: {ti.dag_id}, Run: {ti.run_id}, Task: {ti.task_id} "
                    f"failed\n")
  repo.create_issue(
      title=f'error_dag',
      body=f'{error_msg}',
  )


def on_failure():
  return GithubOperator(
      task_id="on_failure",
      github_conn_id="github_default",
      github_method="get_repo",
      github_method_args={
          # Make Variables
          "full_name_or_id": "CIeNET-International/ml-auto-solutions-2",
      },
      result_processor=create_issue,
      trigger_rule=TriggerRule.ONE_FAILED,
      do_xcom_push=False, # Make try not to serialize Issue into XCom
  )


with models.DAG(
    dag_id="test_on_failure",
    schedule=None,
    tags=[
      "test_dag",
    ],
    catchup=False,
    default_args={
        'retries': 0,  # ← 這裡直接指定 Task 重試次數
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
  with TaskGroup('group1') as gp1:
    taskA = task_a()
  with TaskGroup('group2') as gp2:
    taskB = task_b()
  taskC = task_c()
  onFailure = on_failure()
  gp1 >> gp2 >> taskC >> onFailure
