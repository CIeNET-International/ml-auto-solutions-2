from google.cloud import bigquery, container_v1, storage
from google.api_core.exceptions import NotFound
from datetime import datetime, timezone
import json
import uuid
import os
import re
from oauth2client.service_account import ServiceAccountCredentials
from kubernetes import client, config
from config import BQ_PROJECT_ID, BQ_DATASET, GCS_BUCKET_NAME

# --- Step 1: Config ---
project_id = BQ_PROJECT_ID
dataset_id = BQ_DATASET
view_name = "cluster_view"
table_id = "gke_cluster_info"
bucket_name = GCS_BUCKET_NAME
blob_path = f"tmp/gke_cluster_info_{uuid.uuid4()}.jsonl"
max_rows = 0  # Set <= 0 to fetch all rows

# --- Step 2: Get rows from BQ view ---
def get_clusters_from_view():
    client = bigquery.Client(project=project_id)
    limit_clause = f"LIMIT {max_rows}" if max_rows > 0 else ""
    query = f"""
        SELECT project_name, cluster_name, region
        FROM `{project_id}.{dataset_id}.{view_name}`
        {limit_clause}
    """
    rows = list(client.query(query).result())
    print(f"Fetched {len(rows)} rows from view.")
    return rows


# --- Step 3: List clusters from GKE API ---
def list_clusters_for_project(project_id):
    client = container_v1.ClusterManagerClient()
    parent = f"projects/{project_id}/locations/-"
    response = client.list_clusters(request={"parent": parent})
    return response.clusters if response and response.clusters else []


# --- Step 4: Get detailed cluster status (Enhanced to include status_message) ---
def get_cluster_status(project_id, location, cluster_name):
    client = container_v1.ClusterManagerClient()
    name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
    request = container_v1.GetClusterRequest(name=name)
    cluster = client.get_cluster(request=request)
    cluster_mode = "Autopilot" if cluster.autopilot.enabled else "Standard"

    node_pools_info = []
    if cluster_mode == "Standard":
      for np in cluster.node_pools:
        #node_count = sum(ig.instance_count for ig in np.instance_groups) if np.instance_groups else 0
        node_count = None
        #accelerators_list = [] #comment out, most of nodelpools have no such information
        #accelerator_type = "CPU"
        #if np.config.accelerators:
        #    for accelerator in np.config.accelerators:
        #        # Create a dictionary for each accelerator and append it to the list
        #        accelerator_info = {
        #            'accelerator_type': accelerator.accelerator_type,
        #            'accelerator_count': accelerator.accelerator_count
        #        }
        #        accelerators_list.append(accelerator_info)
        #        if "tpu" in accelerator.accelerator_type.lower():
        #            accelerator_type = "TPU"
        #        elif "gpu" in accelerator.accelerator_type.lower() or "nvidia" in accelerator.accelerator_type.lower():
        #            accelerator_type = "GPU"
        #elif "tpu" in np.config.machine_type.lower():
        #    accelerator_type = "TPU"            
        machine_type = np.config.machine_type if np.config else None
        mt = machine_type.lower()
        machine_family = "CPU"

        # TPU rule
        if re.search(r'(^ct|tpu|v2|v3|v4)', mt):
            machine_family = "TPU"
        # GPU rule
        elif re.search(r'(nvidia|gpu|a100|t4|v100|k80|l4|h100)', mt):
            machine_family = "GPU"

        node_pools_info.append({
            "name": np.name,
            "status": container_v1.NodePool.Status(np.status).name if np.status else "UNKNOWN",
            "status_message": np.status_message or None, 
            "version": np.version,
            "autoscaling_enabled": np.autoscaling.enabled if np.autoscaling else False,
            "initial_node_count": np.initial_node_count,
            "node_count": node_count,
            "machine_type": np.config.machine_type if np.config else None,
            "machine_family": machine_family,
            #"accelerators": accelerators_list,
            #"accelerator_type": accelerator_type,
            "disk_size_gb": np.config.disk_size_gb if np.config else None,
            "preemptible": np.config.preemptible if np.config else False,
        })

    return {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "region": location,
        "status": container_v1.Cluster.Status(cluster.status).name if cluster.status else "UNKNOWN",
        "cluster_mode": cluster_mode,
        "status_message": cluster.status_message or None,
        "node_pools": node_pools_info,
    }

def get_gke_configured_nodepool(project_id: str, region: str, cluster_id: str, nodepool_id: str):
    """
    Get node pool config info from GKE API (desired size, autoscaling range).
    """
    client_gke = container_v1.ClusterManagerClient()
    parent = f"projects/{project_id}/locations/{region}/clusters/{cluster_id}/nodePools/{nodepool_id}"
    nodepool = client_gke.get_node_pool(name=parent)

    configured = {
        "initial_node_count": nodepool.initial_node_count,
        "autoscaling": {
            "enabled": nodepool.autoscaling.enabled if nodepool.autoscaling else False,
            "min_node_count": nodepool.autoscaling.min_node_count if nodepool.autoscaling else None,
            "max_node_count": nodepool.autoscaling.max_node_count if nodepool.autoscaling else None,
        },
    }
    return configured

def get_actual_node_count(nodepool_name: str):
    """
    Get actual number of nodes currently running in the node pool (from Kubernetes API).
    """
    # Load kubeconfig (works locally or in cluster with service account)
    try:
        config.load_kube_config()  # outside cluster
    except:
        config.load_incluster_config()  # inside cluster

    v1 = client.CoreV1Api()
    label_selector = f"cloud.google.com/gke-nodepool={nodepool_name}"
    nodes = v1.list_node(label_selector=label_selector).items
    return len(nodes)

def get_nodepool_summary(project_id, region, cluster_id, nodepool_id):
    configured = get_gke_configured_nodepool(project_id, region, cluster_id, nodepool_id)
    actual = get_actual_node_count(nodepool_id)

    return {
        "nodepool": nodepool_id,
        "configured": configured,
        "actual_running": actual,
    }




# --- Step 5: Build BQ schema (Enhanced to include status_message) ---
def get_bq_schema():
    return [
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cluster_mode", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("op_region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("load_time", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_message", "STRING", mode="NULLABLE"),
        bigquery.SchemaField(
            "node_pools", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("status_message", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("version", "STRING"),
                bigquery.SchemaField("autoscaling_enabled", "BOOLEAN"),
                bigquery.SchemaField("initial_node_count", "INTEGER"),
                bigquery.SchemaField("node_count", "INTEGER"),
                bigquery.SchemaField("machine_type", "STRING"),
                bigquery.SchemaField("machine_family", "STRING"),
                #bigquery.SchemaField("accelerator_type", "STRING"),
                #bigquery.SchemaField(
                #    "accelerators", "RECORD", mode="REPEATED", 
                #    fields=[
                #        bigquery.SchemaField("accelerator_type", "STRING", mode="NULLABLE"),
                #        bigquery.SchemaField("accelerator_count", "INTEGER", mode="NULLABLE"),
                #    ],
                #),
                bigquery.SchemaField("disk_size_gb", "INTEGER"),
                bigquery.SchemaField("preemptible", "BOOLEAN"),
            ]
        )
    ]

# --- Step 6: Upload data to GCS as .jsonl ---
def upload_json_to_gcs(rows):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    jsonl = "\n".join(json.dumps(row) for row in rows)
    blob.upload_from_string(jsonl, content_type="application/json")
    print(f"Uploaded to GCS: gs://{bucket_name}/{blob_path}")


# --- Step 7: Load GCS file to BigQuery ---
def load_jsonl_to_bq(schema):
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    uri = f"gs://{bucket_name}/{blob_path}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()
    print(f"Loaded data to BigQuery table {dataset_id}.{table_id}")


# --- Step 8: Gspread function ---
def insert_gspread_rows(rows):
    try:
        scope = ['https://spreadsheets.google.com/feeds',
                 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(GSPREAD_CREDS_PATH, scope)
        gspread_client = gspread.authorize(creds)
        sheet = gspread_client.open_by_key(GSPREAD_SHEET_ID)
        worksheet = sheet.worksheet(GSPREAD_WORKSHEET_NAME)

        print(f"Inserting {len(rows)} rows into Google Sheet...")
        # Appends new rows to the existing sheet.
        worksheet.append_rows(rows, value_input_option='USER_ENTERED')
        print(f"Successfully appended rows to Google Sheet.")

    except Exception as e:
        print(f"An error occurred while writing to Google Sheet: {e}")

# --- Step 9: Main process ---
def save():
    start_date = datetime.now()
    rows = get_clusters_from_view()
    if not rows:
        print("No rows found in view.")
        return

    rows_without_project = [r for r in rows if not r.get("project_name")]
    rows_without_region = [r for r in rows if not r.get("region")]
    if (rows_without_project):
        print(f'Some rows without project!!')        
        return
    if (rows_without_region):
        print(f'Some rows without region!!')        
        return

    result_rows = []
    gspread_rows_to_insert = []
    load_time = datetime.now(timezone.utc).isoformat()

    for row in rows:
        proj = row.project_name
        cname = row.cluster_name
        location = row.region

        cluster_status = "NOT EXIST"
        cluster_status_message = None
        cluster_mode = None
        node_pools_data = []

        now_utc = datetime.now(timezone.utc).isoformat()

        try:
            info = get_cluster_status(proj, location, cname)
            cluster_status = info["status"]
            cluster_status_message = info["status_message"]
            cluster_mode = info["cluster_mode"]
            node_pools_data = info["node_pools"]

            # Check cluster status for Gspread
            if cluster_status != "RUNNING":
                print(f'Found NOT RUNNING cluster:{proj}.{location}.{cname}')
            for np in node_pools_data:
                if np["status"] != "RUNNING":
                    print(f'Found NOT RUNNING nodepool cluster:{proj}.{location}.{cname}.{np["name"]}')
        except NotFound:
            cluster_status = "NOT EXIST"
            print(f'Found NOT EXIST cluster:{proj}.{location}.{cname}')
        except Exception as e:
            print(f"Error fetching details for {proj}.{location}.{cname}: {e}")
            cluster_status = "ERROR"
            cluster_status_message = str(e)
        result_rows.append({
            "project_id": proj,
            "cluster_name": cname,
            "cluster_mode": cluster_mode,
            "region": location,
            "load_time": load_time,
            "status": cluster_status,
            "status_message": cluster_status_message,
            "node_pools": node_pools_data,
        })

    # Save to GCS and then BQ
    if result_rows:
        upload_json_to_gcs(result_rows)
        load_jsonl_to_bq(get_bq_schema())
    else:
        print("No data to process for BigQuery load.")

    # Clean up the local file if it was created
    if 'blob_path' in locals() and os.path.exists(blob_path):
        os.remove(blob_path)

    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')

# --- Run ---
if __name__ == "__main__":
    save()
