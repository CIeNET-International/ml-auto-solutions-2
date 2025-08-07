from google.cloud import bigquery, container_v1, storage
from google.api_core.exceptions import NotFound
import json
from datetime import datetime, timezone

# --- Step 1: Config ---
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_2"
view_name = "cluster_view"
table_id = "gke_cluster_info"
bucket_name = "amy-xlml-poc"
blob_path = "tmp/gke_cluster_info.jsonl"
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


# --- Step 4: Get detailed cluster status ---
def get_cluster_status(project_id, location, cluster_name):
    client = container_v1.ClusterManagerClient()
    name = f"projects/{project_id}/locations/{location}/clusters/{cluster_name}"
    request = container_v1.GetClusterRequest(name=name)
    cluster = client.get_cluster(request=request)

    node_pools_info = []
    for np in cluster.node_pools:
        node_pools_info.append({
            "name": np.name,
            "status": container_v1.NodePool.Status(np.status).name if np.status else "UNKNOWN",
            "version": np.version,
            "autoscaling_enabled": np.autoscaling.enabled if np.autoscaling else False,
            "initial_node_count": np.initial_node_count,
            "machine_type": np.config.machine_type if np.config else None,
            "disk_size_gb": np.config.disk_size_gb if np.config else None,
            "preemptible": np.config.preemptible if np.config else False,
        })

    return {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "region": location,
        "status": container_v1.Cluster.Status(cluster.status).name if cluster.status else "UNKNOWN",
        "node_pools": node_pools_info,
    }


# --- Step 5: Build BQ schema ---
def get_bq_schema():
    return [
        bigquery.SchemaField("project_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("op_region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("load_time", "TIMESTAMP"),
        bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
        bigquery.SchemaField(
            "node_pools", "RECORD", mode="REPEATED",
            fields=[
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("version", "STRING"),
                bigquery.SchemaField("autoscaling_enabled", "BOOLEAN"),
                bigquery.SchemaField("initial_node_count", "INTEGER"),
                bigquery.SchemaField("machine_type", "STRING"),
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


# --- Step 8: Main process ---
def main():
    rows = get_clusters_from_view()
    if not rows:
        print("No rows found in view.")
        return

    cluster_locs = {}
    projects = set(row.project_name for row in rows)

    print(f"Fetching clusters from {len(projects)} distinct projects...")

    for idx, project in enumerate(projects, 1):
        try:
            clusters = list_clusters_for_project(project)
            print(f"[{idx}/{len(projects)}] {project}: {len(clusters)} clusters")
            for c in clusters:
                key = f"{project}::{c.name}"
                cluster_locs[key] = c.location
        except Exception as e:
            print(f"Error listing clusters for {project}: {e}")

    result_rows = []
    load_time = datetime.now(timezone.utc).isoformat()

    for row in rows:
        proj = row.project_name
        cname = row.cluster_name
        op_region = row.region
        key = f"{proj}::{cname}"
        location = cluster_locs.get(key)

        if not location:
            result_rows.append({
                "project_id": proj,
                "cluster_name": cname,
                "op_region": op_region,
                "region": None,
                "load_time": load_time,
                "status": "NOT EXIST",
                "node_pools": [],
            })
            continue

        try:
            info = get_cluster_status(proj, location, cname)
            result_rows.append({
                "project_id": proj,
                "cluster_name": cname,
                "op_region": op_region,
                "region": info["region"],
                "load_time": load_time,
                "status": info["status"],
                "node_pools": info["node_pools"],
            })
        except NotFound:
            result_rows.append({
                "project_id": proj,
                "cluster_name": cname,
                "op_region": op_region,
                "load_time": load_time,
                "region": location,
                "status": "NOT EXIST",
                "node_pools": [],
            })
        except Exception as e:
            print(f"Error fetching details for {proj}/{cname}: {e}")
            result_rows.append({
                "project_id": proj,
                "cluster_name": cname,
                "op_region": op_region,
                "region": location,
                "status": "ERROR",
                "node_pools": [],
            })

    # Save to GCS and then BQ
    upload_json_to_gcs(result_rows)
    load_jsonl_to_bq(get_bq_schema())


# --- Run ---
if __name__ == "__main__":
    main()

