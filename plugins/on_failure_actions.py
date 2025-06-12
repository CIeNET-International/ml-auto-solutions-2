import logging
import os

from airflow.exceptions import AirflowException
from airflow.listeners import hookimpl
from airflow.models import DagRun
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.github.hooks.github import GithubHook

from dags.common.vm_resource import Project
from xlml.utils import composer
from urllib import parse

_PROJECT_ID = Project.CLOUD_ML_AUTO_SOLUTIONS.value
_REPO_NAME = "GoogleCloudPlatform/ml-auto-solutions"


def generate_dag_run_link(
    proj_id: str,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
):
  airflow_link = composer.get_airflow_url(
    proj_id,
    os.environ.get("COMPOSER_LOCATION"),
    os.environ.get("COMPOSER_ENVIRONMENT"),
  )
  airflow_dag_run_link = (
    f"{airflow_link}/dags/{dag_id}/"
    f"grid?dag_run_id={parse.quote(dag_run_id)}&task_id={task_id}"
  )
  return airflow_dag_run_link


"""
Airflow Listener class to handle DAG run failures.

This listener specifically triggers actions when a DAG run fails.
It checks for the 'on_failure_alert' tag on the failed DAG. If the tag is present,
it proceeds to create a GitHub issue with details about the failed DAG run
and its failed tasks. The issue is assigned to the owners of the failed tasks
(excluding 'airflow' as an owner).

TODO: Implement more sophisticated issue filing strategies beyond a single failure, such as:
-   Consecutive Failures: Only file an issue if a DAG has failed for two or more
    consecutive runs to reduce noise from transient issues.
-   Reduced Pass Rate: File an issue if the current run failed AND the overall
    pass rate of the past N (e.g., 10) runs for this DAG falls below a certain threshold
    (e.g., 50%).
-   Duplicate Issue Prevention: Before filing a new issue, check if a similar
    issue for the same DAG (or error type) has already been filed within a recent
    timeframe (e.g., the past 30 days) to avoid creating duplicate alerts.
"""


class DagRunListener:
  @hookimpl
  def on_dag_run_failed(self, dag_run: DagRun, msg: str):
    log_prefix = self.__class__.__name__
    logging.error(f"[{log_prefix}] DAG run: {dag_run.dag_id} failed")
    logging.error(f"[{log_prefix}] msg: {msg}")

    try:
      # Only DAGs with the 'on_failure_alert' tag will be processed.
      if "on_failure_alert" not in dag_run.dag.tags:
        logging.error(
          f"[{log_prefix}] DAG {dag_run.dag_id} isn't "
          f"'on_failure_alert' by tags. Return"
          )
        return
      logging.error(
        f"[{log_prefix}] DAG run {dag_run.dag_id} is "
        f"'on_failure_alert'"
        )
      environment_name = os.environ.get(
        "COMPOSER_ENVIRONMENT",
        default="ml-automation-solutions"
      )
      logging.error(f"[{log_prefix}] env: {environment_name}")
      title = f"[{log_prefix}] DAG {dag_run.dag_id} failed"
      body = (
        f"DAG: {dag_run.dag_id}\n"
        f"Run: {dag_run.run_id}\n"
        f"Execution date: {dag_run.execution_date}\n"
      )
      for ti in dag_run.task_instances:
        if ti.state != 'failed':
          continue
        logging.error(
          f"[{log_prefix}] Task {ti.dag_id}-{ti.task_id} failed: "
        )
        link = generate_dag_run_link(
          proj_id=_PROJECT_ID,
          dag_id=dag_run.dag_id,
          dag_run_id=dag_run.run_id,
          task_id=ti.task_id
        )
        body += f"Task {ti.task_id} failed, link:\n{link}\n"
      assignees = list(
        {
          task.owner for task in dag_run.dag.tasks if task.owner != 'airflow'
        }
      )
      logging.error(f"[{log_prefix}] assignees: [{assignees}]")
      conn_id = environment_name + "-github_default"
      client = GithubHook(github_conn_id=conn_id).get_conn()
      repo = client.get_repo(full_name_or_id=_REPO_NAME)
      repo.create_issue(
        title=f'{title}',
        body=f'{body}',
        assignees=assignees
      )
      logging.error(f"[{log_prefix}] over")
    except AirflowException as airflow_e:
      logging.error(f"[{log_prefix}] Airflow exception: {airflow_e}")
    except Exception as e:
      logging.error(f"[{log_prefix}] Unexpected exception: {e}")


class ListenerPlugins(AirflowPlugin):
  name = "listener_plugins"
  listeners = [DagRunListener()]
