from google.cloud import bigquery, container_v1, storage
from google.api_core.exceptions import NotFound
import json
import uuid
import tempfile
import os
from datetime import datetime, timezone

# --- Step 1: Config ---
BIGQUERY_CLUSTER_NAME_VIEW = "cluster_info_view_latest"
BIGQUERY_PROJECT_ID = "cienet-cmcs"
BIGQUERY_DATASET = "amy_xlml_poc_prod"
BIGQUERY_TABLE = "gke_list_clusters"
GCS_BUCKET_NAME = "amy-xlml-poc-prod"

BIGQUERY_SCHEMA = [
    bigquery.SchemaField("project_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED"),
]

# --- Step 2: Get rows from BQ view ---
def get_clusters_from_view():
    client = bigquery.Client(project=BIGQUERY_PROJECT_ID)
    query = f"""
        SELECT project_name, cluster_name
        FROM `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_CLUSTER_NAME_VIEW}`
    """
    rows = list(client.query(query).result())
    print(f"Fetched {len(rows)} rows from view.")
    return rows


# --- Step 3: List clusters from GKE API ---
def list_clusters_for_project(project_id):
    try:
        client = container_v1.ClusterManagerClient()
        # The hyphen '-' represents all locations.
        parent = f"projects/{project_id}/locations/-"
        response = client.list_clusters(request={"parent": parent})
        return response.clusters if response and response.clusters else []
    except Exception as e:
        print(f"Error listing clusters for project {project_id}: {e}")
        return []


def main():
    start_date = datetime.now()
    rows = get_clusters_from_view()
    if not rows:
        print("No rows found in view.")
        return

    all_clusters_data = []
    #projects = set(row.project_name for row in rows)
    projects = [
        "tpu-prod-env-one-vm",
    	"tpu-prod-env-multipod",
	    "cloud-tpu-multipod-dev",
	    "supercomputer-testing",
    ]

    print(f"Fetching clusters from {len(projects)} distinct projects...")
    # 1. Collect data from all projects
    for idx, project_id in enumerate(projects, 1):
        try:
            clusters = list_clusters_for_project(project_id)
            print(f"[{idx}/{len(projects)}] {project_id}: {len(clusters)} clusters")
            for c in clusters:
              all_clusters_data.append({
                "project_name": project_id,
                "cluster_name": c.name,
              })
        except Exception as e:
            print(f"Error listing clusters for {project}: {e}")

    if not all_clusters_data:
        print("No clusters found. Exiting.")
        return

    # 2. Prepare and upload data to GCS
    print("Writing data to a temporary local file...")
    
    # Create a unique filename for the GCS object to avoid conflicts
    temp_file_name = f"gke_clusters_data_{uuid.uuid4()}.jsonl"
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    
    # Use a temporary file for safe handling
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as temp_local_file:
        for row in all_clusters_data:
            json.dump(row, temp_local_file)
            temp_local_file.write('\n')
        local_file_path = temp_local_file.name

    print(f"Uploading temporary file to GCS: gs://{GCS_BUCKET_NAME}/{temp_file_name}")
    blob = bucket.blob(temp_file_name)
    try:
        blob.upload_from_filename(local_file_path)
    finally:
        # Clean up the local temporary file
        os.remove(local_file_path)
        print("Temporary local file deleted.")

    # 3. Load data from GCS into BigQuery with TRUNCATE disposition
    print("Loading data from GCS into BigQuery...")
    bq_client = bigquery.Client()
    table_ref = bq_client.dataset(BIGQUERY_DATASET, project=BIGQUERY_PROJECT_ID).table(BIGQUERY_TABLE)

    job_config = bigquery.LoadJobConfig(
        schema=BIGQUERY_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Truncates the table
    )
    
    # GCS URI for the uploaded file
    uri = f"gs://{GCS_BUCKET_NAME}/{temp_file_name}"
    
    try:
        load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)
        print(f"Load job {load_job.job_id} started. Waiting for completion...")
        load_job.result()  # Wait for the job to complete
        
        print(f"Load job completed successfully! {load_job.output_rows} rows loaded into {BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}")
    except Exception as e:
        print(f"BigQuery load job failed: {e}")
    finally:
        # Clean up the temporary GCS file after the load job is done.
        # This is a good practice to avoid accumulating files in your bucket.
        print(f"Deleting GCS object: {uri}")
        blob.delete()
        print("GCS object deleted.")

# --- Run ---
if __name__ == "__main__":
    main()


