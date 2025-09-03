import re
import json
import urllib.parse
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage, logging_v2
from config_log import (
    BQ_PROJECT_ID, BQ_DATASET, BQ_VIEW_NAME_NO_CLUSTER, BQ_DEST_TABLE_CLUSTER_FROM_LOG,
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
    LOGS_PROJECT_ID
)


def parse_zulu(dt_str):
    """Parse Zulu-format datetime like 2025-08-10T00:00:00Z"""
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def extract_execution_date_from_run_id(run_id):
    """Extract date from Airflow run_id like manual__2025-08-10T00:00:00+00:00"""
    match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", run_id)
    if match:
        return datetime.fromisoformat(match.group(1)).replace(tzinfo=timezone.utc)
    return None

def extract_date_string_from_run_id(run_id):
    match = re.search(r'__(.*)', run_id)
    if match:
        return match.group(1)
    return None

def build_log_filter(dag_id, run_id, task_id_prefix, exec_date, start_time, end_time, search_keyword):
    extracted_duration = extract_date_string_from_run_id(run_id)
    """Build log explorer filter string."""
    filter_parts = [
        f'resource.type="cloud_composer_environment"',
        f'resource.labels.environment_name="ml-automation-solutions"',
        f'labels.workflow="{dag_id}"',
        f'labels.execution-date="{extracted_duration}"',
        f'labels.task-id=~"^{task_id_prefix}.*"',
        f'timestamp >= "{start_time.isoformat()}"',
        f'timestamp <= "{end_time.isoformat()}"',
        f'SEARCH("{search_keyword}")'
    ]
    return "\n".join(filter_parts)

def query_logs(filter_str, log_project_id, page_size):
    """Query logs from Cloud Logging."""
    client = logging_v2.Client(project=log_project_id)
    entries = list(client.list_entries(
        filter_=filter_str,
        page_size=page_size,
    ))
    return entries


def save_to_gcs_and_load_bq(data, schema):
    """Save data to GCS as NDJSON and load to BigQuery with truncate mode."""
    storage_client = storage.Client(project=GCS_PROJECT_ID)
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)

    timestamp_str = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    gcs_path = f"logs_export_{timestamp_str}.json"
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)

    # Write as NDJSON (one JSON object per line)
    ndjson_data = "\n".join(json.dumps(row, default=json_serial) for row in data)
    blob.upload_from_string(ndjson_data, content_type="application/json")

    uri = f"gs://{GCS_BUCKET_NAME}/{gcs_path}"
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_DEST_TABLE_CLUSTER_FROM_LOG}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(data)} rows to {table_id}.")


def get_cluster_info_from_logs(dag_id, run_id, test_id, test_start_date, test_end_date):
    cluster_name = None
    project_name = None
    filter_str = build_log_filter(
        dag_id, run_id, test_id, extract_execution_date_from_run_id(run_id),
        test_start_date, test_end_date, "gcloud container clusters get-credentials"
    )

    payload_str = None
    pattern = re.compile(r"gcloud container clusters get-credentials\s+(\S+)")
    entries = query_logs(filter_str, LOGS_PROJECT_ID, 1)
    for e in entries:
        payload_str = e.payload if hasattr(e, "payload") else str(e)
        match = pattern.search(payload_str)
        if match:
            cluster_name = match.group(1)
            break

    if (cluster_name):
        #pattern_project = re.compile(r"gcloud config set project (\S+)")
        #pattern_project = re.compile(r"gcloud config set project\s+([^']+)")
        pattern_project_1 = re.compile(r"gcloud\s+config\s+set\s+project\s+([a-z0-9-]+)")
        pattern_project_2 = re.compile(
            r"gcloud\s+container\s+clusters\s+get-credentials\s+[^\s]+"
            r"(?:\s+--region\s+[^\s]+)?"
            r"\s+--project\s+([a-z0-9-]+)"
        )
        #match = pattern_project.search(payload_str)
        #if match:
        #    project_name = match.group(1)
        for regex in (pattern_project_1, pattern_project_2):
            match = regex.search(payload_str)
            if match:
                project_name = match.group(1)
                break
    return cluster_name, project_name

def process_test(dag_id, run_id, execution_date, test_id, test_start_date, test_end_date):
    """Process a single test, fetch logs, and build the output dictionary."""
    project_name = None
    cluster_name = None
    region = None

    if not test_start_date or not test_end_date:
        print(f"Skipping {dag_id} / {test_id} due to missing start/end date.")
        return cluster_name, project_name, region

    cluster_name, project_name = get_cluster_info_from_logs(dag_id, run_id, test_id, test_start_date, test_end_date)
    print(f'dag:{dag_id},test_id:{test_id},cluster_name:{cluster_name},project:{project_name}')            

    return cluster_name, project_name, region


def main():
    start_date = datetime.now()
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)

    query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}`"
    rows = list(bq_client.query(query).result())

    schema = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("task_id", "STRING"),
        bigquery.SchemaField("test_id", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("cluster_name", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("project_name", "STRING"),
    ]

    output_data = []
    for row in rows:
        
        cluster_name, project_name, region = process_test(row.dag_id, row.run_id, row.execution_date, row.test_id, row.test_start_date, row.test_end_date)

        if cluster_name and project_name:
          output_data.append({
            "dag_id": row.dag_id,
            "task_id": row.test_id,
            "test_id": row.test_id,
            "run_id": row.run_id,
            "cluster_name": cluster_name,
            "region": region,
            "project_name":project_name 
          })

    save_to_gcs_and_load_bq(output_data, schema)
    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


if __name__ == "__main__":
    main()
