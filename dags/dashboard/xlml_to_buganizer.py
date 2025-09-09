# ================================================================
# Step 0: Imports & Global Config
# ================================================================
import enum
import json
import logging
from typing import List, Any, Dict
from datetime import datetime, timezone

import google
from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, container_v1

# --- Airflow DAG Schedule ---
SCHEDULED_TIME = None

# --- BigQuery Config ---
DEFAULT_PROJECT_ID = "cienet-cmcs"
DEFAULT_DATASET_ID = "amy_xlml_poc_prod"
BQ_VIEW_NAME = "cluster_view"

# --- Google Sheets Config ---
GSPREAD_INSERT_ENABLED = False
DEFAULT_GSPREAD_SHEET_ID = "1FkyzvC0HGxFPGjIpCV4pWHB_QFWUOtLPIlDfnBNvt4Q"
GSPREAD_WORKSHEET_NAME = "unhealthy_clusters"

# --- GCP Auth ---
credentials, _ = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)


# ================================================================
# Step 1: Enums & Constants
# ================================================================
class ClusterType(enum.Enum):
  CLUSTER = "Cluster"
  NODE_POOL = "NodePool"


class ClusterStatus(enum.Enum):
  NOT_EXIST = "NOT EXIST"
  ERROR = "ERROR"


NOT_FOUND_MSG = "Cluster not found in GKE API."


# ================================================================
# Step 2: Helper Functions (Row builders / Logging / GSheet)
# ================================================================
def build_malfunction_cluster_row(
    proj: str,
    cname: str,
    cluster_status: str,
    cluster_status_message: str,
    now_utc: str,
) -> List[str]:
  return [
      ClusterType.CLUSTER.value,
      proj,
      cname,
      cluster_status,
      cluster_status_message,
      "",
      now_utc,
  ]


def build_malfunction_nodepool_row(
    proj: str,
    cname: str,
    cluster_status: str,
    cluster_status_message: str,
    node_pools: List[Dict[str, Any]],
    now_utc: str,
) -> List[str]:
  if node_pools is None:
    node_pools = []
  return [
      ClusterType.NODE_POOL.value,
      proj,
      cname,
      cluster_status,
      cluster_status_message,
      json.dumps(node_pools),
      now_utc,
  ]


def build_malfunction_nodepool_json(
    node_pools: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
  mal_node_pools = []
  for node_pool in node_pools:
    if node_pool["status"] != "RUNNING":
      mal_node_pool = {
          "name": node_pool["name"],
          "status": node_pool["status"],
          "status_message": node_pool["status_message"],
          "version": node_pool["version"],
          "autoscaling_enabled": node_pool["autoscaling_enabled"],
          "initial_node_count": node_pool["initial_node_count"],
          "machine_type": node_pool["machine_type"],
          "disk_size_gb": node_pool["disk_size_gb"],
          "preemptible": node_pool["preemptible"],
      }
      mal_node_pools.append(mal_node_pool)
  return mal_node_pools


def print_failed_cluster_info(cluster_status_rows):
  """Log failed cluster info for debugging"""
  result_rows_list = []
  for cluster_status_row in cluster_status_rows:
    result_row = {
        "type": cluster_status_row[0],
        "project_id": cluster_status_row[1],
        "cluster_name": cluster_status_row[2],
        "status": cluster_status_row[3],
        "status_message": cluster_status_row[4],
        "node_pools": cluster_status_row[5],
        "load_time": cluster_status_row[6],
    }
    result_rows_list.append(result_row)
  logging.info(f"result: {result_rows_list}")


def insert_gspread_rows(rows: List[List[str]]):
  """Insert rows into Google Sheet via Airflow GSheetsHook"""
  try:
    hook = GSheetsHook(gcp_conn_id="google_cloud_default")
    print(f"Inserting {len(rows)} rows into Google Sheet...")
    hook.append_values(
        spreadsheet_id=DEFAULT_GSPREAD_SHEET_ID,
        range_=GSPREAD_WORKSHEET_NAME,
        values=rows,
        insert_data_option="INSERT_ROWS",
        value_input_option="RAW",
    )
    print(f"Successfully appended rows to Google Sheet.")
  except Exception as e:
    print(f"An error occurred while writing to Google Sheet: {e}")


# ================================================================
# Step 3: BigQuery Functions
# ================================================================
def get_clusters_from_view() -> List[Any]:
  """Get cluster list from BigQuery view"""
  client = bigquery.Client(project=DEFAULT_PROJECT_ID, credentials=credentials)
  query = f"""
        SELECT project_name, cluster_name
        FROM `{DEFAULT_PROJECT_ID}.{DEFAULT_DATASET_ID}.{BQ_VIEW_NAME}`
    """
  rows = list(client.query(query).result())
  logging.info(f"Fetched {len(rows)} rows from view.")
  return rows


# ================================================================
# Step 4: GKE Functions
# ================================================================
def list_clusters_by_project(project_id) -> List[Any]:
  """List all clusters in a given GCP project"""
  client = container_v1.ClusterManagerClient(credentials=credentials)
  parent = f"projects/{project_id}/locations/-"
  response = client.list_clusters(request={"parent": parent})
  return response.clusters if response and response.clusters else []


def get_cluster_status(project_id, location, cluster_name) -> Dict[str, Any]:
  """Get detailed GKE cluster + nodepool status"""
  client = container_v1.ClusterManagerClient(credentials=credentials)
  name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
  request = container_v1.GetClusterRequest(name=name)
  cluster = client.get_cluster(request=request)

  node_pools_info = []
  for np in cluster.node_pools:
    node_pools_info.append(
        {
            "name": np.name,
            "status": container_v1.NodePool.Status(np.status).name
            if np.status
            else "UNKNOWN",
            "status_message": np.status_message or None,
            "version": np.version,
            "autoscaling_enabled": np.autoscaling.enabled
            if np.autoscaling
            else False,
            "initial_node_count": np.initial_node_count,
            "machine_type": np.config.machine_type if np.config else None,
            "disk_size_gb": np.config.disk_size_gb if np.config else None,
            "preemptible": np.config.preemptible if np.config else False,
        }
    )

  return {
      "project_id": project_id,
      "region": location,
      "cluster_name": cluster_name,
      "status": container_v1.Cluster.Status(cluster.status).name
      if cluster.status
      else "UNKNOWN",
      "status_message": cluster.status_message or None,
      "node_pools": node_pools_info,
  }


# ================================================================
# Step 5: Workflow Functions
# ================================================================
def fetch_clusters_list():
  clusters = get_clusters_from_view()
  if not clusters:
    logging.info("No rows found in view.")
    return []
  return clusters


def fetch_clusters_location(clusters):
  """Map project::cluster_name -> location"""
  cluster_locations = {}
  projects = set(cluster.project_name for cluster in clusters)
  logging.info(f"Fetching clusters from {len(projects)} distinct projects...")
  for idx, project in enumerate(projects, 1):
    try:
      clusters = list_clusters_by_project(project)
      logging.info(
          f"[{idx}/{len(projects)}] {project}: {len(clusters)} clusters"
      )
      for cluster in clusters:
        key = f"{project}::{cluster.name}"
        cluster_locations[key] = cluster.location
    except Exception as e:
      logging.info(f"Error listing clusters for {project}: {e}")
  return cluster_locations


def insert_cluster_status_lists(
    status_list, project_name, location, cluster_name, now_utc
):
  """Insert both cluster + nodepool status into list"""
  try:
    info = get_cluster_status(project_name, location, cluster_name)
    cluster_status = info["status"]
    cluster_status_message = info["status_message"]
    node_pools_data = info["node_pools"]

    if cluster_status != "RUNNING":
      status_list.append(
          build_malfunction_cluster_row(
              proj=project_name,
              cname=cluster_name,
              cluster_status=cluster_status,
              cluster_status_message=cluster_status_message,
              now_utc=now_utc,
          )
      )
    mal_node_pools = build_malfunction_nodepool_json(node_pools_data)
    if len(mal_node_pools) > 0:
      status_list.append(
          build_malfunction_nodepool_row(
              proj=project_name,
              cname=cluster_name,
              cluster_status=cluster_status,
              cluster_status_message=cluster_status_message,
              node_pools=mal_node_pools,
              now_utc=now_utc,
          )
      )
  except NotFound:
    status_list.append(
        build_malfunction_cluster_row(
            proj=project_name,
            cname=cluster_name,
            cluster_status=ClusterStatus.NOT_EXIST.value,
            cluster_status_message=NOT_FOUND_MSG,
            now_utc=now_utc,
        )
    )
  except Exception as e:
    logging.info(
        f"Error fetching details for {project_name}/{cluster_name}: {e}"
    )
    status_list.append(
        build_malfunction_cluster_row(
            proj=project_name,
            cname=cluster_name,
            cluster_status=ClusterStatus.ERROR.value,
            cluster_status_message=str(e),
            now_utc=now_utc,
        )
    )


def fetch_clusters_status(clusters_list, cluster_locations):
  """Fetch status for all clusters & node pools"""
  cluster_status_rows = []
  now_utc = datetime.now(timezone.utc).isoformat()
  for cluster in clusters_list:
    project_name = cluster.project_name
    cluster_name = cluster.cluster_name
    key = f"{project_name}::{cluster_name}"
    location = cluster_locations.get(key)
    if location:
      insert_cluster_status_lists(
          cluster_status_rows, project_name, location, cluster_name, now_utc
      )
    else:
      cluster_status_rows.append(
          build_malfunction_cluster_row(
              proj=project_name,
              cname=cluster_name,
              cluster_status=ClusterStatus.NOT_EXIST.value,
              cluster_status_message=NOT_FOUND_MSG,
              now_utc=now_utc,
          )
      )
  print_failed_cluster_info(cluster_status_rows)
  return cluster_status_rows


# ================================================================
# Step 6: Airflow Tasks
# ================================================================
@task
def pull_clusters_status():
  target_clusters_list = fetch_clusters_list()
  target_clusters_locations = fetch_clusters_location(target_clusters_list)
  return fetch_clusters_status(target_clusters_list, target_clusters_locations)


@task
def insert_gsheet_rows_task(gspread_rows_to_insert):
  if GSPREAD_INSERT_ENABLED and gspread_rows_to_insert:
    insert_gspread_rows(gspread_rows_to_insert)


# ================================================================
# Step 7: DAG Definition
# ================================================================
params = {
    "source_bq_project_id": Param(
        type="string",
        title="Source BigQuery GCP Project ID",
        description="The Source Google Cloud Project ID where the big query belong to",
        default=DEFAULT_PROJECT_ID,
    ),
    "source_bq_dataset_id": Param(
        type="string",
        title="Source Big Query Dataset ID",
        description="The Source BigQuery dataset ID where the data would be queried",
        default=DEFAULT_DATASET_ID,
    ),
    "target_gsheet_id": Param(
        type="string",
        title="Target Google Sheet ID",
        description="The Target Google Sheet ID where the Buganizer issue information are stored",
        default=DEFAULT_GSPREAD_SHEET_ID,
    ),
}

with DAG(
    dag_id="xlml_to_buganizer",
    description="""TBF""",
    start_date=datetime(2025, 9, 3),
    schedule_interval=SCHEDULED_TIME,
    catchup=False,
    tags=[],
    default_args={"retries": 0},
    params=params,
) as dag:
  target_clusters_status = pull_clusters_status()
  insert_gsheet_rows_task(target_clusters_status)
