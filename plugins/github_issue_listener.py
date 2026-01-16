import fnmatch
import json
import logging
import os
import time
from typing import Iterable, List, Dict, Any, Set
from urllib import parse

import google.auth.transport.requests
import jwt
import requests
from airflow.exceptions import AirflowException
from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from github import Github, Auth
from github.Issue import Issue
from google.cloud import secretmanager, storage

# Constants
PROJECT_ID = os.environ.get("GCP_PROJECT", "cloud-ml-auto-solutions")
REPO_NAME = "GoogleCloudPlatform/ml-auto-solutions"
SECRET_MANAGER_ID = (
    "airflow-connections-"
    + os.environ.get("COMPOSER_ENVIRONMENT", default="ml-automation-solutions")
    + "-github_app"
)
ALLOW_LIST_PATH = "plugins/allow_list.txt"
BLOCK_LIST_PATH = "plugins/block_list.txt"
CONFIG_PATH = "plugins/config.json"


def get_github_client() -> Github:
  """Initializes and returns the GitHub API client."""
  secret_value = fetch_json_secret()
  app_id = secret_value.get("app_id")
  installation_id = secret_value.get("installation_id")
  private_key = secret_value.get("private_key")

  if not all([app_id, installation_id, private_key]):
    secret_path = (
        f"projects/{PROJECT_ID}/secrets/{SECRET_MANAGER_ID}/versions/latest"
    )
    raise AirflowException(
        "[GithubIssueListener] One or more required keys ('app_id',"
        f" 'installation_id', 'private_key') not found in secret: {secret_path}"
    )

  installation_token = get_installation_token(
      app_id, installation_id, private_key
  )
  return Github(auth=Auth.Token(installation_token))


def fetch_json_secret() -> Dict[str, Any]:
  """Fetches and parses a JSON secret from Google Secret Manager."""
  client = secretmanager.SecretManagerServiceClient()
  secret_path = (
      f"projects/{PROJECT_ID}/secrets/{SECRET_MANAGER_ID}/versions/latest"
  )
  response = client.access_secret_version(request={"name": secret_path})
  secret_str = response.payload.data.decode("UTF-8")
  logging.info("[GithubIssueListener] Secret value received.")
  return json.loads(secret_str)


def get_installation_token(
    github_app_id: str, installation_id: str, private_key: str
) -> str:
  """Retrieves a GitHub App installation access token."""
  jwt_token = generate_jwt(github_app_id, private_key)
  token_url = (
      f"https://api.github.com/app/installations/{installation_id}/access_tokens"
  )
  headers = {
      "Authorization": f"Bearer {jwt_token}",
      "Accept": "application/vnd.github+json",
  }
  resp = requests.post(token_url, headers=headers)
  resp.raise_for_status()
  logging.info("[GithubIssueListener] GitHub token received")
  return resp.json()["token"]


def generate_jwt(github_app_id: str, private_key: str) -> str:
  """Generates a short-lived JWT for GitHub App authentication."""
  now = int(time.time())
  jwt_payload = {"iat": now - 60, "exp": now + (10 * 60), "iss": github_app_id}
  return jwt.encode(jwt_payload, private_key, algorithm="RS256")


def query_latest_issues(client: Github, title: str) -> Issue | None:
  """Queries for the most recently updated open issue with a given title."""
  query = f"{title} in:title state:open repo:{REPO_NAME} is:issue"
  issues = list(client.search_issues(query=query, sort="updated", order="desc"))
  return issues[0] if issues else None


def add_comment_or_create_issue(
    client: Github,
    issue: Issue | None,
    title: str,
    issue_body: str,
    assignees: List[str] | None = None,
):
  """Adds a comment to an existing issue or creates a new one."""
  if issue:
    logging.info(
        f"[GithubIssueListener] Found existing issue #{issue.number}, adding a"
        " comment."
    )
    issue.create_comment(body=issue_body)
  else:
    logging.info(
        f"[{GithubIssueListener.log_prefix}] No existing issue found, creating a"
        " new one."
    )
    repo = client.get_repo(full_name_or_id=REPO_NAME)
    repo.create_issue(title=title, body=issue_body, assignees=assignees or [])


def read_items_from_gcs(bucket_name: str, blob_name: str) -> set[str]:
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)
  content = blob.download_as_text(encoding="utf-8")
  lines = [line.strip() for line in content.splitlines() if line.strip()]
  return set(lines)


def read_json_from_gcs(bucket_name: str, blob_name: str) -> Dict[str, Any]:
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(blob_name)
  content = blob.download_as_text(encoding="utf-8")
  return json.loads(content)


class GithubIssueListener:
  def __init__(self):
    self.log_prefix = self.__class__.__name__

  @hookimpl
  def on_dag_run_success(self, dag_run: DagRun, msg: str):
    self.on_dag_finished(dag_run, msg)

  @hookimpl
  def on_dag_run_failed(self, dag_run: DagRun, msg: str):
    self.on_dag_finished(dag_run, msg)

  def on_dag_finished(self, dag_run: DagRun, msg: str):
    logging.info(f"[{self.log_prefix}] DAG run: {dag_run.dag_id} finished")
    logging.info(f"[{self.log_prefix}] msg: {msg}")

    try:
      if not self._is_dag_enabled(dag_run):
        logging.info(
            f"[{self.log_prefix}] DAG {dag_run.dag_id} is not enabled for"
            " this listener. Skipping."
        )
        return

      failed_task_instances = [
          ti for ti in dag_run.task_instances if ti.state == "failed"
      ]
      if not failed_task_instances:
        logging.info(
            f"[{self.log_prefix}] No failed tasks, GitHub Issue operation"
            " completed."
        )
        return

      logging.info(
          f"[{self.log_prefix}] Failed tasks found. Preparing to send GitHub"
          " Issue."
      )

      base_issue_body = (
          f"- **Run ID**: {dag_run.run_id}\n"
          f"- **Execution Date**: {dag_run.execution_date}\n"
      )

      test_name_dict = {}
      for task_instance in failed_task_instances:
        test_name = GithubIssueListener.get_test_name(task_instance)
        if test_name in test_name_dict:
          test_name_dict[test_name].append(task_instance)
        else:
          test_name_dict[test_name] = [task_instance]

      client = get_github_client()
      for test_name, task_instances in test_name_dict.items():
        title = f"[{self.log_prefix}] {dag_run.dag_id} {test_name} failed"
        assignees = set()

        issue_body_details = []
        for task_instance in task_instances:
          link = self.generate_dag_run_link(
              proj_id=str(PROJECT_ID),
              dag_id=dag_run.dag_id,
              dag_run_id=dag_run.run_id,
              task_id=task_instance.task_id,
          )
          issue_body_details.append(
              f"- **Failed Task**: [{task_instance.task_id}]({link})\n"
          )
          if task_instance.task.owner and task_instance.task.owner != "airflow":
            assignees.add(task_instance.task.owner)

        full_issue_body = base_issue_body + "".join(issue_body_details)
        issue = query_latest_issues(client, title)

        try:
          add_comment_or_create_issue(
              client, issue, title, full_issue_body, list(assignees)
          )
        except Exception as e:
          if "422" in str(e):  # Invalid GitHub username as assignees
            logging.warning(
                f"[{self.log_prefix}] Invalid assignees {assignees}, retrying"
                f" without them. Original error: {e}"
            )
            add_comment_or_create_issue(client, issue, title, full_issue_body)
          else:
            raise

      logging.info(f"[{self.log_prefix}] GitHub Issue operation completed.")

    except AirflowException as airflow_e:
      logging.error(
          f"[{self.log_prefix}] Airflow exception: {airflow_e}", exc_info=True
      )
    except Exception as e:
      logging.error(
          f"[{self.log_prefix}] Unexpected exception: {e}", exc_info=True
      )

  def _is_dag_enabled(self, dag_run: DagRun) -> bool:
    """Checks if the DAG is enabled based on allow/block lists."""
    enable_plugin = None
    if GithubIssueListener.is_in_allow_list(dag_run):
      enable_plugin = True
      logging.info(
          f"[{self.log_prefix}] DAG {dag_run.dag_id} is in allow_list.txt"
      )

    if GithubIssueListener.is_in_block_list(dag_run):
      enable_plugin = False
      logging.info(
          f"[{self.log_prefix}] DAG {dag_run.dag_id} is in block_list.txt"
      )

    default_enabled = GithubIssueListener.enable_plugin_by_default()

    return enable_plugin is not False and (
        enable_plugin is True or default_enabled
    )

  @staticmethod
  def get_test_name(task_instance: TaskInstance) -> str:
    task = task_instance.task
    if task.task_group and task.task_group.group_id:
      return task.task_group.group_id.split(".")[0]
    return task.task_id

  @staticmethod
  def generate_dag_run_link(
      proj_id: str, dag_id: str, dag_run_id: str, task_id: str
  ) -> str:
    airflow_link = GithubIssueListener.get_airflow_url(
        proj_id,
        os.environ.get("COMPOSER_LOCATION"),
        os.environ.get("COMPOSER_ENVIRONMENT"),
    )
    return (
        f"{airflow_link}/dags/{dag_id}/"
        f"grid?dag_run_id={parse.quote(dag_run_id)}&task_id={task_id}&tab=logs"
    )

  @staticmethod
  def get_airflow_url(project: str, region: str, composer_env: str) -> str:
    """Get Airflow web UI."""
    request_endpoint = (
        "https://composer.googleapis.com/"
        f"v1beta1/projects/{project}/locations/"
        f"{region}/environments/{composer_env}"
    )
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(google.auth.transport.requests.Request())
    headers = {"Authorization": f"Bearer {creds.token}"}
    response = requests.get(request_endpoint, headers=headers)
    response.raise_for_status()
    configs = response.json()
    return configs["config"]["airflowUri"]

  @staticmethod
  def is_in_allow_list(dag_run: DagRun) -> bool:
    allow_items = read_items_from_gcs(
        os.environ.get("GCS_BUCKET"), ALLOW_LIST_PATH
    )
    return GithubIssueListener.is_in_list(allow_items, dag_run)

  @staticmethod
  def is_in_block_list(dag_run: DagRun) -> bool:
    block_items = read_items_from_gcs(
        os.environ.get("GCS_BUCKET"), BLOCK_LIST_PATH
    )
    return GithubIssueListener.is_in_list(block_items, dag_run)

  @staticmethod
  def is_in_list(items: Set[str], dag_run: DagRun) -> bool:
    return (
        GithubIssueListener.contains_id(items, dag_run.dag_id)
        or GithubIssueListener.has_any_tag(items, dag_run.dag.tags)
        or GithubIssueListener.matches_pattern(items, dag_run.dag_id)
    )

  @staticmethod
  def enable_plugin_by_default() -> bool:
    config = read_json_from_gcs(os.environ.get("GCS_BUCKET"), CONFIG_PATH)
    return config.get("enable_plugin_by_default", False)

  @staticmethod
  def _parse_items_by_key(
      items: Set[str], target_key: str
  ) -> Iterable[str]:
    """A generator to parse and yield values for a specific key."""
    for item in items:
      parts = item.split(":", 1)
      if len(parts) == 2:
        key = parts[0].lower().strip()
        value = parts[1].strip()
        if key == target_key and value:
          yield value

  @staticmethod
  def contains_id(items: Set[str], target_id: str) -> bool:
    id_generator = GithubIssueListener._parse_items_by_key(items, "id")
    return any(an_id == target_id for an_id in id_generator)

  @staticmethod
  def has_any_tag(items: Set[str], target_tags: List[str]) -> bool:
    tag_set = set(GithubIssueListener._parse_items_by_key(items, "tag"))
    return not tag_set.isdisjoint(target_tags)

  @staticmethod
  def matches_pattern(items: Set[str], target_id: str) -> bool:
    patterns = GithubIssueListener._parse_items_by_key(items, "pattern")
    return any(fnmatch.fnmatch(target_id, pattern) for pattern in patterns)
