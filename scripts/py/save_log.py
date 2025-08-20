import re
import json
import urllib.parse
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage, logging_v2
from config_log import (
    BQ_PROJECT_ID, BQ_DATASET, BQ_VIEW_NAME, BQ_DEST_TABLE,
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
    LOGS_PROJECT_ID, LOG_EXPLORER_HOST,
    LOG_QUERY_END_PADDING_SECONDS, LOG_QUERY_SEVERITY,
    HACKED_DAG_TIMES, DAGS_TO_QUERY_LOGS
)


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

def build_log_filter(dag_id, run_id, task_id_prefix, exec_date, start_time, end_time, severity=None):
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
        filter_parts.append(f'severity={severity}')
    return "\n".join(filter_parts)


def build_log_explorer_url(filter_str):
    """Generate URL-encoded Cloud Logging query URL."""
    encoded_filter = urllib.parse.quote(filter_str, safe='')
    return f"{LOG_EXPLORER_HOST};query={encoded_filter}?project={LOGS_PROJECT_ID}"


def query_logs(filter_str):
    """Query logs from Cloud Logging."""
    client = logging_v2.Client(project=LOGS_PROJECT_ID)
    entries = list(client.list_entries(
        filter_=filter_str,
        order_by=logging_v2.DESCENDING  # kept as-is; code reverses later to get ascending
    ))
    return entries


def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


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
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_DEST_TABLE}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(data)} rows to {table_id}.")


def main():
    start_date = datetime.now()
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)

    query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME}`"
    rows = list(bq_client.query(query).result())

    output_data = []
    schema = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("execution_date", "TIMESTAMP"),
        bigquery.SchemaField("run_start_date", "TIMESTAMP"),
        bigquery.SchemaField("run_end_date", "TIMESTAMP"),
        bigquery.SchemaField("run_status", "STRING"),
        bigquery.SchemaField("tests", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("test_id", "STRING"),
            bigquery.SchemaField("test_start_date", "TIMESTAMP"),
            bigquery.SchemaField("test_end_date", "TIMESTAMP"),
            bigquery.SchemaField("test_status", "STRING"),
            bigquery.SchemaField("logs", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("payload", "STRING"),
                bigquery.SchemaField("process", "STRING"),
                bigquery.SchemaField("task_id", "STRING"),
                bigquery.SchemaField("try_number", "STRING"),
                bigquery.SchemaField("worker_id", "STRING"),
                #bigquery.SchemaField("workflow", "STRING"),
            ]),
            bigquery.SchemaField("log_url_error", "STRING"),
            bigquery.SchemaField("log_url_all", "STRING")
        ])
    ]

    for row in rows:
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
            #continue
            execution_date = extract_execution_date_from_run_id(run_id) or row.execution_date
            run_start_date = row.run_start_date
            run_end_date = row.run_end_date

        tests_data = []
        for test in row["tests"]:
            test_id = test["test_id"]
            test_status = test["test_status"]

            if dag_id in HACKED_DAG_TIMES and test_id in HACKED_DAG_TIMES[dag_id].get("tests", {}):
                hack_test = HACKED_DAG_TIMES[dag_id]["tests"][test_id]
                test_start_date = parse_zulu(hack_test["start"])
                test_end_date = parse_zulu(hack_test["end"])
            else:
                test_start_date = test["test_start_date"]
                test_end_date = test["test_end_date"]

            # Skip if either start or end is None
            if not test_start_date or not test_end_date:
                print(f"Skipping {dag_id} / {test_id} due to missing start/end date.")
                continue

            print(f"Processing {dag_id} / {test_id} test_duration:{test_start_date}-{test_end_date}")

            end_with_padding = test_end_date + timedelta(seconds=LOG_QUERY_END_PADDING_SECONDS)
            filter_exec_date = execution_date

            logs_entries = []
            if (not DAGS_TO_QUERY_LOGS or dag_id in DAGS_TO_QUERY_LOGS) and (test_status != 'success'):
                filter_str_error = build_log_filter(
                    dag_id, run_id, test_id, filter_exec_date,
                    test_start_date, end_with_padding,
                    severity=LOG_QUERY_SEVERITY
                )
                print(f"query log explorer {filter_str_error}")
                entries = query_logs(filter_str_error)
                for e in reversed(entries):
                    labels = e.labels or {}
                    logs_entries.append({
                        "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                        "payload": e.payload if hasattr(e, "payload") else str(e),
                        "process": labels.get("process"),
                        "try_number": labels.get("try-number") or labels.get("try_number"),
                        "task_id": labels.get("task-id") or labels.get("task_id"),
                        "worker_id": labels.get("worker-id") or labels.get("worker_id"),
                        #"workflow": labels.get("workflow")
                    })

            filter_str_error = build_log_filter(
                dag_id, run_id, test_id, filter_exec_date,
                test_start_date, end_with_padding,
                severity=LOG_QUERY_SEVERITY
            )
            filter_str_all = build_log_filter(
                dag_id, run_id, test_id, filter_exec_date,
                test_start_date, end_with_padding
            )
            log_url_error = build_log_explorer_url(filter_str_error) if test_status != 'success' else ""
            log_url_all = build_log_explorer_url(filter_str_all)

            tests_data.append({
                "test_id": test_id,
                "test_start_date": test_start_date,
                "test_end_date": test_end_date,
                "test_status": test["test_status"],
                "logs": logs_entries,
                "log_url_error": log_url_error,
                "log_url_all": log_url_all
            })

        output_data.append({
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": execution_date,
            "run_start_date": run_start_date,
            "run_end_date": run_end_date,
            "run_status": row.run_status,
            "tests": tests_data
        })

    save_to_gcs_and_load_bq(output_data, schema)
    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


if __name__ == "__main__":
    main()

