import fnmatch
import json
import logging
import os
from typing import Iterable, List, Dict, Any, Set
from urllib import parse

import google.auth
import google.auth.transport.requests
import requests
from airflow.exceptions import AirflowException
from airflow.listeners import hookimpl
from airflow.models import DagRun, TaskInstance
from google.cloud import secretmanager, storage
from googleapiclient.discovery import build

# Constants
PROJECT_ID = os.environ.get("GCP_PROJECT", "cloud-ml-auto-solutions")
SECRET_MANAGER_ID = (
    "airflow-connections-"
    + os.environ.get("COMPOSER_ENVIRONMENT", default="ml-automation-solutions")
    + "-github_app"
)
ALLOW_LIST_PATH = "plugins/allow_list.txt"
BLOCK_LIST_PATH = "plugins/block_list.txt"
CONFIG_PATH = "plugins/config.json"


def get_google_sheet_service():
  """Initializes and returns the Google Sheets API service client."""
  credentials, project = google.auth.default(
      scopes=["https://www.googleapis.com/auth/spreadsheets"]
  )
  credentials.refresh(google.auth.transport.requests.Request())
  service = build("sheets", "v4", credentials=credentials)
  logging.info("[GoogleSheetListener] Google Sheets service client created.")
  return service


def get_google_sheet_id() -> str:
  """Fetches the Google Sheet ID from Secret Manager."""
  client = secretmanager.SecretManagerServiceClient()
  secret_path = (
      f"projects/{PROJECT_ID}/secrets/{SECRET_MANAGER_ID}/versions/latest"
  )
  response = client.access_secret_version(request={"name": secret_path})
  secret_str = response.payload.data.decode("UTF-8")
  secret_dict = json.loads(secret_str)
  sheet_id = secret_dict.get("google_sheet_id")
  if not sheet_id:
    raise AirflowException(
        "[GoogleSheetListener] Key 'google_sheet_id' not found in secret:"
        f" {secret_path}"
    )
  logging.info("[GoogleSheetListener] Google Sheet ID received.")
  return sheet_id


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


class GoogleSheetListener:

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
            f"[{self.log_prefix}] No failed tasks, skipping Google Sheet"
            " operation."
        )
        return

      logging.info(
          f"[{self.log_prefix}] Failed tasks found. Preparing to write to"
          " Google Sheet."
      )

      test_name_dict = {}
      for task_instance in failed_task_instances:
        test_name = GoogleSheetListener.get_test_name(task_instance)
        if test_name in test_name_dict:
          test_name_dict[test_name].append(task_instance)
        else:
          test_name_dict[test_name] = [task_instance]

      rows_to_append = []
      for test_name, task_instances in test_name_dict.items():
        failed_task_ids = ",".join(
            [
                f"{ti.task_id}: {self.generate_dag_run_link(proj_id=str(PROJECT_ID), dag_id=dag_run.dag_id, dag_run_id=dag_run.run_id, task_id=ti.task_id)}"
                for ti in task_instances
            ]
        )
        row = [
            dag_run.dag_id,
            test_name,
            failed_task_ids,
            str(dag_run.execution_date),
            "jackyf@google.com"
        ]
        rows_to_append.append(row)

      if rows_to_append:
        sheet_service = get_google_sheet_service()
        spreadsheet_id = get_google_sheet_id()
        body = {"values": rows_to_append}
        sheet_service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range="A2",
            valueInputOption="USER_ENTERED",
            body=body,
        ).execute()

        logging.info(
            f"[{self.log_prefix}] Successfully wrote {len(rows_to_append)} rows"
            " to Google Sheet."
        )

    except AirflowException as airflow_e:
      logging.error(
          f"[{self.log_prefix}] Airflow exception: {airflow_e}",
          exc_info=True,
      )
    except Exception as e:
      logging.error(
          f"[{self.log_prefix}] Unexpected exception: {e}", exc_info=True
      )

  def _is_dag_enabled(self, dag_run: DagRun) -> bool:
    """Checks if the DAG is enabled based on allow/block lists."""
    enable_plugin = None
    if GoogleSheetListener.is_in_allow_list(dag_run):
      enable_plugin = True
      logging.info(
          f"[{self.log_prefix}] DAG {dag_run.dag_id} is in allow_list.txt"
      )

    if GoogleSheetListener.is_in_block_list(dag_run):
      enable_plugin = False
      logging.info(
          f"[{self.log_prefix}] DAG {dag_run.dag_id} is in block_list.txt"
      )

    default_enabled = GoogleSheetListener.enable_plugin_by_default()

    return enable_plugin is not False and (
        enable_plugin is True or default_enabled
    )

  @staticmethod
  def get_test_name(task_instance: TaskInstance):
    task = task_instance.task
    if task.task_group and task.task_group.group_id:
      return task.task_group.group_id.split(".")[0]
    return task.task_id

  @staticmethod
  def generate_dag_run_link(
      proj_id: str, dag_id: str, dag_run_id: str, task_id: str
  ):
    airflow_link = GoogleSheetListener.get_airflow_url(
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
    configs = response.json()
    return configs["config"]["airflowUri"]

  @staticmethod
  def is_in_allow_list(dag_run: DagRun) -> bool:
    allow_items = read_items_from_gcs(
        os.environ.get("GCS_BUCKET"), ALLOW_LIST_PATH
    )
    return GoogleSheetListener.is_in_list(allow_items, dag_run)

  @staticmethod
  def is_in_block_list(dag_run: DagRun) -> bool:
    block_items = read_items_from_gcs(
        os.environ.get("GCS_BUCKET"), BLOCK_LIST_PATH
    )
    return GoogleSheetListener.is_in_list(block_items, dag_run)

  @staticmethod
  def is_in_list(items: Set[str], dag_run: DagRun):
    return (
        GoogleSheetListener.contains_id(items, dag_run.dag_id)
        or GoogleSheetListener.has_any_tag(items, dag_run.dag.tags)
        or GoogleSheetListener.matches_pattern(items, dag_run.dag_id)
    )

  @staticmethod
  def enable_plugin_by_default() -> bool:
    config = read_json_from_gcs(os.environ.get("GCS_BUCKET"), CONFIG_PATH)
    return config.get("enable_plugin_by_default", False)

  @staticmethod
  def _parse_items_by_key(items: Set[str], target_key: str) -> Iterable[str]:
    """A generator to parse and yield values for a specific key."""
    for item in items:
      parts = item.split(":", 1)
      if len(parts) == 2:
        key = parts[0].lower().strip()
        value = parts[1].strip()
        if key == target_key and value:
          yield value

  @staticmethod
  def contains_id(items: Set[str], target_id: str):
    id_generator = GoogleSheetListener._parse_items_by_key(items, "id")
    return any(an_id == target_id for an_id in id_generator)

  @staticmethod
  def has_any_tag(items: Set[str], target_tags: List[str]):
    tag_set = set(GoogleSheetListener._parse_items_by_key(items, "tag"))
    return not tag_set.isdisjoint(target_tags)

  @staticmethod
  def matches_pattern(items: Set[str], target_id: str):
    patterns = GoogleSheetListener._parse_items_by_key(items, "pattern")
    return any(fnmatch.fnmatch(target_id, pattern) for pattern in patterns)
