from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.models import DagRun, Variable
from airflow.providers.github.hooks.github import GithubHook
import logging


def validate_listener_config(config: dict):
  if "enabled" in config and "full_repo_name" in config:
    return
  raise AirflowException("Invalid config from Secret Manager. Please check")


def query_listener_config(
    prefix: str
):
  config = Variable.get(prefix + "-github_listener_config", deserialize_json=True)
  validate_listener_config(config)
  return config

"""
Notes
1. Get github accounts as assignee from default_args of a scheduled DAG
and params of a manual DAG. Unable to get test_owner from DagRun object.
2. Check the importance of 'prefix' 
github_listener_config:
{
  "enabled": bool,
  "full_repo_name": "<owner>/<repo_name>"
}
"""
class GithubListener:
  @hookimpl
  def on_dag_run_failed(self, dag_run: DagRun, msg: str):
    logging.error(f"[GithubListener] DAG run: {dag_run.dag_id} failed")
    logging.error(f"[GithubListener] msg: {msg}")

    try:
      if "github_enabled" not in dag_run.dag.tags:
        logging.error(f"[GithubListener] DAG {dag_run.dag_id} isn't "
                      f"github-enabled by tags. Return")
        return
      prefix = "severus"  # Need to the real scenario
      listener_config = query_listener_config(prefix)
      if not listener_config["enabled"]:
        logging.error(f"[GithubListener] DAG {dag_run.dag_id} isn't "
                      f"github-enabled by config. Return")
        return

      logging.error(f"[GithubListener] DAG run {dag_run.dag_id} is "
                    f"github-enabled")
      title = f"[GithubListener] DAG {dag_run.dag_id} failed"
      body = (
          f"DAG: {dag_run.dag_id},\n"
          f"Run: {dag_run.run_id},\n"
          f"Execution date: {dag_run.execution_date},\n"
      )
      for ti in dag_run.task_instances:
        if ti.state == 'failed':
          logging.error(
              f"[GithubListener] Task {ti.dag_id}-{ti.task_id} failed: "
          )
          body += f"Task {ti.task_id} failed,\n"

      client = GithubHook(github_conn_id="github_default").get_conn()
      repo_name = listener_config['full_repo_name']
      repo = client.get_repo(full_name_or_id=repo_name)
      repo.create_issue(
          title=f'{title}',
          body=f'{body}',
      )
      logging.error("[GithubListener] over")
    except AirflowException as airflow_e:
      logging.error(f"[GithubListener] Airflow exception: {airflow_e}")
    except Exception as e:
      logging.error(f"[GithubListener] Unexpected exception: {e}")


class ListenerPlugins(AirflowPlugin):
  name = "listener_plugins"
  listeners = [GithubListener()]
