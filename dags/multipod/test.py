
from google.cloud import logging as log_explorer
from datetime import datetime, timezone, timedelta
from typing import Optional
from absl import logging


def validate_log_with_gcs(
    project_id: str,
    location: str,
    cluster_name: str,
    bucket_name: str,
    namespace: str = "default",
    pod_pattern: str = "*",
    container_name: Optional[str] = None,
    text_filter: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> bool:
  """Validate the workload log is training correct"""
  entries = list_log_entries(
      project_id=project_id,
      location=location,
      cluster_name=cluster_name,
      namespace=namespace,
      pod_pattern=pod_pattern,
      container_name=container_name,
      text_filter=text_filter,
      start_time=start_time,
      end_time=end_time,
  )
  find_str = " to backup/gcs/"
  find_step = "step "
  gcs_save_step_list = []
  for entry in entries:
    if entry.payload is not None:
      payload_str = str(entry.payload)
      for line in payload_str.split("\n"):
        print(line)
        start_index = line.find(find_str)
        if start_index != -1:
          folder_index = start_index + len(find_str)
          gcs_checkpoint_path = line[folder_index:]
          print(gcs_checkpoint_path)
          step_index = line.find(find_step)
          if step_index != -1:
            step_number_index = step_index + len(find_step)
            step = line[step_number_index:start_index]
            if step not in gcs_save_step_list:
              gcs_save_step_list.append(int(step))
  print(max(gcs_save_step_list))
        
  # if len(vali_step_list) == len(new_step_list):
  #   logging.info("Validate success")
  #   return True
  # else:
  #   raise

def validate_log_with_step(
    project_id: str,
    location: str,
    cluster_name: str,
    namespace: str = "default",
    pod_pattern: str = "*",
    container_name: Optional[str] = None,
    text_filter: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    vali_step_list: Optional[list] = None,
    validation_string: Optional[str] = None
) -> bool:
  """Validate the workload log is training correct"""
  entries = list_log_entries(
      project_id=project_id,
      location=location,
      cluster_name=cluster_name,
      namespace=namespace,
      pod_pattern=pod_pattern,
      container_name=container_name,
      text_filter=text_filter,
      start_time=start_time,
      end_time=end_time,
  )
  if validation_string is None or vali_step_list is None:
    logging.info("Validate input can not found")
    return False
  new_step_list = []
  for entry in entries:
    if entry.payload is not None:
      payload_str = str(entry.payload)
      for line in payload_str.split("\n"):
        print(line)
        if vali_step_list is not None:
          for step in vali_step_list:
            vali_str = validation_string + str(step)
            if vali_str in line and step not in new_step_list:
              logging.info(f"├─ Timestamp: {entry.timestamp}")
              logging.info("└─ Payload:")
              logging.info(f"   {line}")
              new_step_list.append(step)
  if len(vali_step_list) == len(new_step_list):
    logging.info("Validate success")
    return True
  else:
    raise


def list_log_entries(
    project_id: str,
    location: str,
    cluster_name: str,
    namespace: str = "default",
    pod_pattern: str = "*",
    container_name: Optional[str] = None,
    text_filter: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> list:
  """
  List log entries for the specified Google Cloud project.
  This function connects to Google Cloud Logging,
  constructs a filter for Kubernetes container logs
  within a specific project, location, cluster, namespace,
  and pod name pattern, and retrieves log
  entries from the specified time range.
  It prints the timestamp, severity, resource information,
  and payload for each log entry found.
  Args:
      project_id: The Google Cloud project ID
      location: GKE cluster location
      cluster_name: GKE cluster name
      namespace: Kubernetes namespace (defaults to "default")
      pod_pattern: Pattern to match pod names (defaults to "*")
      container_name: Optional container name to filter logs
      text_filter: Optional comma-separated string to
      filter log entries by textPayload content
      start_time: Optional start time for log retrieval
      (defaults to 12 hours ago)
      end_time: Optional end time for log retrieval (defaults to now)
  Returns:
      bool: Number of log entries found
  """

  # Create a Logging Client for the specified project
  logging_client = log_explorer.Client(project=project_id)

  # Set the time window for log retrieval: default to last 12 hours if not provided
  if end_time is None:
    end_time = datetime.now(timezone.utc)
  if start_time is None:
    start_time = end_time - timedelta(hours=12)

  # Format times as RFC3339 UTC "Zulu" format required by the Logging API
  start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
  end_time_str = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")

  # Construct the log filter
  log_filter = (
      f'resource.labels.project_id="{project_id}" '
      f'resource.labels.location="{location}" '
      f'resource.labels.cluster_name="{cluster_name}" '
      f'resource.labels.namespace_name="{namespace}" '
      f'resource.labels.pod_name:"{pod_pattern}" '
      "severity>=DEFAULT "
      f'timestamp>="{start_time_str}" '
      f'timestamp<="{end_time_str}"'
  )

  # Add container name filter if provided
  if container_name:
    log_filter += f' resource.labels.container_name="{container_name}"'

  # Add text content filter if provided
  if text_filter:
    log_filter += f' SEARCH("{text_filter}")'

    # filter_terms = text_filter.split(",")  # Split by comma
    # for term in filter_terms:
    #   log_filter += f' textPayload:"{term.strip()}"'

  # Retrieve log entries matching the filter
  logging.info(f"Log filter constructed: {log_filter}")
  entries = logging_client.list_entries(filter_=log_filter)

  return entries


def validate_log_exist(
    project_id: str,
    location: str,
    cluster_name: str,
    namespace: str = "default",
    pod_pattern: str = "*",
    container_name: Optional[str] = None,
    text_filter: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
) -> bool:
  """Validate the workload log is training correct"""
  entries = list_log_entries(
      project_id=project_id,
      location=location,
      cluster_name=cluster_name,
      namespace=namespace,
      pod_pattern=pod_pattern,
      container_name=container_name,
      text_filter=text_filter,
      start_time=start_time,
      end_time=end_time,
  )
  log_list = []
  for entry in entries:
    if entry.payload is not None:
      payload_str = str(entry.payload)
      log_list.append(payload_str)
      for line in payload_str.split("\n"):
        logging.info(f"├─ Timestamp: {entry.timestamp}")
        logging.info("└─ Payload:")
        logging.info(f"   {line}")
  if len(log_list) > 0:
    print(len(log_list))
    logging.info("Validate success")
    return log_list
  else:
    raise

# result = validate_log_with_step(
#   project_id="cienet-cmcs",
#   location="us-east5",
#   cluster_name="jf-v5p-8-2",
#   namespace="gke-managed-checkpointing",
#   pod_pattern="multitier-driver",
#   container_name="replication-worker",
#   text_filter="Successful: backup for step",
#   vali_step_list=[0, 5, 9],
#   validation_string="abc")

result = validate_log_exist(
  project_id="cienet-cmcs",
  location="us-east5",
  cluster_name="ernie-cienet-v5p-8",
  namespace="gke-managed-checkpointing",
  pod_pattern="multitier-driver",
  container_name="replication-worker",
  start_time = datetime.now(timezone.utc) - timedelta(hours=24),
  end_time=datetime.now(timezone.utc),
  text_filter="Successful: backup for step",
  # bucket_name="backup/gcs"
)

print(result)

