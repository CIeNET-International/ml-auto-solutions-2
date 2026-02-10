import re
import json
from google.protobuf.json_format import MessageToDict, MessageToJson
import urllib.parse
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage, logging_v2
import time
from dags.dashboard.statistic.config import (
    BQ_PROJECT_ID, BQ_DATASET,
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
    LOG_PROJECT_ID, LOG_EXPLORER_HOST,
    LOG_QUERY_END_PADDING_SECONDS, LOG_QUERY_SEVERITY,
    HACKED_DAG_TIMES, DAGS_TO_QUERY_LOGS
)

# Quota Limit: 200 ReadRequestsPerMinutePerProject.
# Minimum delay between requests: 60 seconds / 200 requests = 0.3 seconds.
# Using 0.35 seconds for a safe buffer.
LOGGING_API_DELAY_SECONDS = 0.4#0.35

def parse_zulu(dt_str):
    """Parse Zulu-format datetime like 2025-08-10T00:00:00Z"""
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


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

def build_log_filter(dag_id, run_id, task_id_prefix, start_time, end_time, severity=None):
    extracted_duration = extract_date_string_from_run_id(run_id)
    """Build log explorer filter string."""
    filter_parts = [
        f'resource.type="cloud_composer_environment"',
        f'resource.labels.environment_name="ml-automation-solutions"',
        f'labels.workflow="{dag_id}"',
        f'labels.execution-date="{extracted_duration}"',
        f'labels.task-id=~"^{task_id_prefix}.*"',
        f'timestamp >= "{start_time.isoformat()}"',
        f'timestamp <= "{end_time.isoformat()}"'
    ]
    if severity:
        filter_parts.append(f'severity>={severity}')
    return "\n".join(filter_parts)

def build_log_filter_errors_plus_keywords(dag_id, run_id, task_id_prefix, start_time, end_time):
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
        f'(severity >= ERROR) OR (',
        f'(textPayload =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"',
        f'  OR jsonPayload.message =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"',
        f') AND (severity = INFO OR severity = NOTICE OR severity = WARNING)'
        f') OR (textPayload =~ "(?i)fatal" OR jsonPayload.message =~ "(?i)fatal")'
    ]
    return "\n".join(filter_parts)



def build_log_filter_workload_id(dag_id, run_id, task_id_prefix, start_time, end_time):
    extracted_duration = extract_date_string_from_run_id(run_id)
    """Build log explorer filter string."""
    filter_parts = [
        f'resource.type="cloud_composer_environment"',
        f'resource.labels.environment_name="ml-automation-solutions"',
        f'labels.workflow="{dag_id}"',
        f'labels.execution-date="{extracted_duration}"',
        f'labels.task-id=~"^{task_id_prefix}.*generate_workload_id"',
        f'timestamp >= "{start_time.isoformat()}"',
        f'timestamp <= "{end_time.isoformat()}"',
        f'SEARCH("Returned value was")'
    ]
    return "\n".join(filter_parts)


def build_log_filter_k8s(dag_id, run_id, task_id_prefix, start_time, end_time, cluster_name, cluster_region, workload_id):
    extracted_duration = extract_date_string_from_run_id(run_id)
    """Build log explorer filter string."""
    filter_parts = [
        f'resource.labels.cluster_name="{cluster_name}"',
        f'resource.labels.location="{cluster_region}"',
        f'(resource.type="k8s_pod" OR resource.type="k8s_node" OR resource.type="k8s_container")',
        f'labels."logging.gke.io/top_level_controller_name"=~"{workload_id}"',
        f'severity>DEFAULT',
        f'timestamp >= "{start_time.isoformat()}"',
        f'timestamp <= "{end_time.isoformat()}"'
    ]
    return "\n".join(filter_parts)

def build_log_filter_keywords(dag_id, run_id, task_id_prefix, start_time, end_time):
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
        f'(',
        f'textPayload =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"',
        f'  OR jsonPayload.message =~ "(?i)error|failed|exception|denied|exceeded|not found|backoff|unschedulable"',
        f')',
        f'severity = INFO OR severity = NOTICE OR severity = WARNING'
    ]
    return "\n".join(filter_parts)


def build_log_explorer_url(filter_str, project_id):
    """Generate URL-encoded Cloud Logging query URL."""
    encoded_filter = urllib.parse.quote(filter_str, safe='')
    return f"{LOG_EXPLORER_HOST};query={encoded_filter}?project={project_id}"


def query_logs(filter_str, log_project_id, page_size):
    """Query logs from Cloud Logging with rate limiting."""
    print(f'QUERY:{filter_str}')
    # Introduce a delay before the API call to respect the quota limit
    time.sleep(LOGGING_API_DELAY_SECONDS) # <--- RATE LIMITING DELAY

    client = logging_v2.Client(project=log_project_id)
    entries = list(client.list_entries(
        filter_=filter_str,
        page_size=page_size,
        #order_by=logging_v2.DESCENDING  # kept as-is; code reverses later to get ascending
    ))
    return entries


def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def save_to_gcs_and_load_bq(data, schema, target_table_name):
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
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{target_table_name}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(data)} rows to {table_id}.")


def get_dag_run_info(row):
    """Extract or parse DAG run dates based on HACKED_DAG_TIMES."""
    dag_id = row.dag_id
    run_id = row.run_id

    if dag_id in HACKED_DAG_TIMES:
        hack_info = HACKED_DAG_TIMES[dag_id]
        run_info = hack_info["run"]
        execution_date = parse_zulu(run_info["execution"])
        run_start_date = parse_zulu(run_info["start"])
        run_end_date = parse_zulu(run_info["end"])
        run_id = run_info.get("run_id", run_id)
    else:
        execution_date = extract_execution_date_from_run_id(run_id) or row.execution_date
        run_start_date = row.run_start_date
        run_end_date = row.run_end_date

    return dag_id, run_id, execution_date, run_start_date, run_end_date


def get_test_run_dates(dag_id, test):
    """Extract or parse test run dates based on HACKED_DAG_TIMES."""
    test_id = test["test_id"]
    if dag_id in HACKED_DAG_TIMES and test_id in HACKED_DAG_TIMES[dag_id].get("tests", {}):
        hack_test = HACKED_DAG_TIMES[dag_id]["tests"][test_id]
        test_start_date = parse_zulu(hack_test["start"])
        test_end_date = parse_zulu(hack_test["end"])
    else:
        test_start_date = test["test_start_date"]
        test_end_date = test["test_end_date"]
    return test_id, test_start_date, test_end_date

