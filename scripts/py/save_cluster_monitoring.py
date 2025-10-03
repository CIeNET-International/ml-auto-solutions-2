from google.cloud import monitoring_v3, logging, bigquery, storage
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
import tempfile
import json
from config import (
    BQ_PROJECT_ID, BQ_DATASET,
    GCS_BUCKET_NAME,
)

# ================= CONFIG =================
project_for_metadata = BQ_PROJECT_ID
dataset = BQ_DATASET
gcs_bucket = GCS_BUCKET_NAME

#report_day = datetime(2025, 10, 1, tzinfo=timezone.utc)  # Replace with target day
bq_client = bigquery.Client()

query = f"""
  SELECT DATE(MAX(run_start_date)) AS report_day
  FROM `{project_for_metadata}.{dataset}.dag_runs_with_logs`
"""

row = list(bq_client.query(query).result())[0]
report_day = datetime.combine(row.report_day, datetime.min.time()).replace(tzinfo=timezone.utc)

print("Report day:", report_day)

# BQ table
table_id = f"{project_for_metadata}.{dataset}.cluster_monitoring"

# 10-minute bucket
bucket_minutes = 5

# ================= TIME RANGE =================
start_time = report_day.replace(hour=0, minute=0, second=0, microsecond=0)
end_time = report_day.replace(hour=23, minute=59, second=59, microsecond=999999)
print(f"Report from {start_time} to {end_time} in a bucket windows {bucket_minutes} mins")

# ================= CLIENTS =================
mon_client = monitoring_v3.MetricServiceClient()
bq_client = bigquery.Client(project=project_for_metadata)

# ================= METRIC FILTERS =================
metric_filters = {
    "accelerator_duty_cycle": {
        "metric": "kubernetes.io/container/accelerator/duty_cycle",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    },
    "accelerator_memory_used": {
        "metric": "kubernetes.io/container/accelerator/memory_used",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    },
    "accelerator_memory_total": {
        "metric": "kubernetes.io/container/accelerator/memory_total",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    },
    "memory_bandwidth_utilization": {
        "metric": "kubernetes.io/container/accelerator/memory_bandwidth_utilization",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    },
    "request_accelerators": {
        "metric": "kubernetes.io/container/accelerator/request",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_COUNT
    },
    "tensorcore_utilization": {
        "metric": "kubernetes.io/container/accelerator/tensorcore_utilization",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    },
    "collective_latencies": {
        "metric": "kubernetes.io/container/multislice/network/collective_end_to_end_latencies",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
    },
    "dcn_transfer_latencies": {
        "metric": "kubernetes.io/container/multislice/network/dcn_transfer_latencies",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
    },
    # districbuted, have to handle these two seperately
    #"device2host_transfer_latencies": {
    #    "metric": "kubernetes.io/container/multislice/network/device_to_host_transfer_latencies",
    #    "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
    #},
    #"host2device_transfer_latencies": {
    #    "metric": "kubernetes.io/container/multislice/network/host_to_device_transfer_latencies",
    #    "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_PERCENTILE_99
    #},
    "restart_count": {
        "metric": "kubernetes.io/container/restart_count",
        "aligner": monitoring_v3.Aggregation.Aligner.ALIGN_DELTA
    },
}

# ================= BQ SCHEMA =================
schema = [
    bigquery.SchemaField("project_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("monitor_day", "DATE", mode="REQUIRED"),
    bigquery.SchemaField(
        "buckets",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("bucket_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField(
                "metrics",
                "RECORD",
                mode="REQUIRED",
                fields=[bigquery.SchemaField(m, "FLOAT") for m in metric_filters.keys()],
            ),
            bigquery.SchemaField(
                "logs",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("reason", "STRING"),
                    bigquery.SchemaField("reason_count", "INTEGER"),
                    bigquery.SchemaField(
                        "messages",
                        "RECORD",
                        mode="REPEATED",
                        fields=[
                            bigquery.SchemaField("message", "STRING"),
                            bigquery.SchemaField("count", "INTEGER"),
                        ],
                    ),
                ],
            ),
            bigquery.SchemaField(
                "cluster_events",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("event_time", "TIMESTAMP"),
                    bigquery.SchemaField("event_type", "STRING"),
                    bigquery.SchemaField("message", "STRING"),
                ],
            ),
        ],
    ),
]

# ================= HELPERS =================
def bucket_interval(ts):
    minute = (ts.minute // bucket_minutes) * bucket_minutes
    return ts.replace(minute=minute, second=0, microsecond=0)  # returns datetime

def fetch_clusters():
    #query = f"SELECT project_id, cluster_name, region FROM `{project_for_metadata}.{dataset}.gke_cluster_info` where cluster_name='bodaborg-v6e-256-lcscld-c'"
    query = f"SELECT project_id, cluster_name, region FROM `{project_for_metadata}.{dataset}.gke_cluster_info` where cluster_name !='cluster-1'"
    rows = bq_client.query(query).result()
    return [{"project_id": r.project_id, "cluster_name": r.cluster_name, "region": r.region} for r in rows]

def get_point_value(point):
    return point.value.double_value if point.value else 0.0

# ================= METRICS =================
def fetch_metrics(project_name, cluster_name, region):
    results_buckets = {k: defaultdict(list) for k in metric_filters.keys()}

    for key, info in metric_filters.items():
        metric_type = info["metric"]
        aligner = info["aligner"]

        interval = monitoring_v3.TimeInterval({"start_time": start_time, "end_time": end_time})
        aggregation = monitoring_v3.Aggregation({
            "alignment_period": {"seconds": bucket_minutes * 60},
            "per_series_aligner": aligner,
        })

        try:
            results = mon_client.list_time_series(
                request={
                    "name": project_name,
                    "filter": f'metric.type="{metric_type}" AND resource.labels.cluster_name="{cluster_name}" AND resource.labels.location="{region}"',
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                    "aggregation": aggregation,
                }
            )
            for ts in results:
                for point in ts.points:
                    ts_dt = point.interval.end_time
                    bucket = bucket_interval(ts_dt)
                    value = get_point_value(point)
                    results_buckets[key][bucket].append(value)
        except Exception as e:
            print(f"⚠️ Metric {metric_type} not found in {project_name}/{cluster_name}: {e}")

    # Compute average per bucket
    bucket_avg = {}
    all_buckets = set()
    for metric in metric_filters.keys():
        all_buckets.update(results_buckets[metric].keys())

    for b in sorted(all_buckets):
        bucket_avg[b] = {
            metric: sum(results_buckets[metric].get(b, [0.0])) / max(len(results_buckets[metric].get(b, [])), 1)
            for metric in metric_filters.keys()
        }

    return bucket_avg

# ================= LOGS =================
def fetch_logs(cluster_project_id, cluster_name, region):
    log_client = logging.Client(project=cluster_project_id)
    logs_buckets = defaultdict(lambda: defaultdict(lambda: {"reason_count": 0, "messages": Counter()}))

    reasons = [
        "FailedScheduling", "NodeNotReady", "BackOff", "FailedMount",
        "FailedAttachVolume", "FailedDetachVolume", "ErrImagePull",
        "ImagePullBackOff", "FailedCreatePodSandBox"
    ]
    reason_filter = " OR ".join([f'jsonPayload.reason="{r}"' for r in reasons])

    filter_str = (
        f'logName="projects/{cluster_project_id}/logs/events" '
        f'AND resource.labels.cluster_name="{cluster_name}" '
        f'AND resource.labels.location="{region}" '
        f'AND (severity="WARNING" OR severity="ERROR") '
        f'AND ({reason_filter}) '
        f'AND timestamp >= "{start_time.isoformat()}" '
        f'AND timestamp <= "{end_time.isoformat()}"'
    )

    entries = log_client.list_entries(filter_=filter_str, page_size=1000)
    for entry in entries:
        bucket = bucket_interval(entry.timestamp)
        payload = entry.payload

        if isinstance(payload, dict):
            reason = payload.get("reason", "UnknownReason")
            message = payload.get("message", "NoMessage")
        else:
            reason = "UnknownReason"
            message = str(payload)

        logs_buckets[bucket][reason]["reason_count"] += 1
        logs_buckets[bucket][reason]["messages"][message] += 1

    return logs_buckets

# ================= CLUSTER EVENTS =================
def fetch_cluster_events(cluster_project_id, cluster_name, region):
    log_client = logging.Client(project=cluster_project_id)
    events = []

    # Audit logs for cluster state transitions
    filter_str = (
        f'logName="projects/{cluster_project_id}/logs/cloudaudit.googleapis.com%2Factivity" '
        f'resource.labels.cluster_name="{cluster_name}" '
        f'resource.labels.location="{region}" '
        f'timestamp >= "{start_time.isoformat()}" '
        f'timestamp <= "{end_time.isoformat()}" '
        f'resource.type=("gke_cluster" OR "gke_nodepool") '
        f'log_id("cloudaudit.googleapis.com/activity") '
        f'protoPayload.status.code != 0 '
        f'protoPayload.methodName:"google.container.cluster."'
    )

    entries = log_client.list_entries(filter_=filter_str, page_size=1000)
    for entry in entries:
        ts = entry.timestamp.isoformat()
        payload = entry.payload
        event_type = getattr(payload, "method_name", "ClusterEvent")
        message = getattr(payload, "status", str(payload))
        events.append({
            "event_time": ts,
            "event_type": event_type,
            "message": message,
        })
    return events

# ================= TRANSFORM TO BQ ROW =================
def transform_report_for_bq(cluster_project_id, cluster_name, region, monitor_day, metrics_buckets, logs_buckets, cluster_events):
    buckets_data = []
    all_bucket_times = sorted(set(metrics_buckets.keys()) | set(logs_buckets.keys()))
    for b in all_bucket_times:
        metrics = metrics_buckets.get(b, {})
        logs_struct = []
        if b in logs_buckets:
            for reason, info in logs_buckets[b].items():
                messages_list = [{"message": m, "count": c} for m, c in info["messages"].items()]
                logs_struct.append({
                    "reason": reason,
                    "reason_count": info["reason_count"],
                    "messages": messages_list
                })

        bucket_struct = {
            "bucket_time": b,
            "metrics": {m: metrics.get(m, 0.0) for m in metric_filters.keys()},
            "logs": logs_struct,
            "cluster_events": cluster_events,
        }
        buckets_data.append(bucket_struct)

    row = {
        "project_name": cluster_project_id,
        "cluster_name": cluster_name,
        "region": region,
        "monitor_day": monitor_day.date(),
        "buckets": buckets_data,
    }
    return row

# ================= SAVE TO GCS =================
def save_report_to_gcs(rows, bucket_name, object_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    with tempfile.NamedTemporaryFile("w", delete=False) as tmpfile:
        for r in rows:
            tmpfile.write(json.dumps(r, default=str) + "\n")
        tmp_path = tmpfile.name
    blob = bucket.blob(object_name)
    blob.upload_from_filename(tmp_path)
    print(f"✅ Report uploaded to gs://{bucket_name}/{object_name}")
    return f"gs://{bucket_name}/{object_name}"

# ================= LOAD / MERGE TO BQ =================
def load_report_to_bq(table_id, gcs_uri):
    client = bigquery.Client()

    # Create table if not exists
    try:
        client.get_table(table_id)
    except:
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)
        print(f"✅ Created table {table_id}")

    # Load to a temporary table first
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    temp_table_id = table_id + "_temp"
    load_job = client.load_table_from_uri(gcs_uri, temp_table_id, job_config=job_config)
    load_job.result()
    print(f"✅ Loaded data into temporary table {temp_table_id}")

    # Merge into main table
    merge_sql = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
    ON T.project_name = S.project_name
       AND T.cluster_name = S.cluster_name
       AND T.monitor_day = S.monitor_day
    WHEN MATCHED THEN
      UPDATE SET buckets = S.buckets
    WHEN NOT MATCHED THEN
      INSERT (project_name, cluster_name, region, monitor_day, buckets)
      VALUES(S.project_name, S.cluster_name, S.region, S.monitor_day, S.buckets)
    """
    client.query(merge_sql).result()
    print(f"✅ Merge complete into {table_id}")

    # Delete temp table
    client.delete_table(temp_table_id)

def save():
    start_date = datetime.now()
    clusters = fetch_clusters()
    report_rows = []

    for c in clusters:
        cluster_project_id, region, cluster_name = c["project_id"], c["region"], c["cluster_name"]
        project_name = f"projects/{cluster_project_id}"

        print(f"\nFetching cluster {cluster_name} ({cluster_project_id}/{region}) ...")
        metrics_buckets = fetch_metrics(project_name, cluster_name, region)
        logs_buckets = fetch_logs(cluster_project_id, cluster_name, region)
        cluster_events = fetch_cluster_events(cluster_project_id, cluster_name, region)
        row = transform_report_for_bq(cluster_project_id, cluster_name, region, report_day, metrics_buckets, logs_buckets, cluster_events)
        report_rows.append(row)

    # Save to GCS
    gcs_object = f"cluster_monitoring/{report_day.date()}.json"
    gcs_uri = save_report_to_gcs(report_rows, gcs_bucket, gcs_object)

    # Load/Merge into BigQuery
    load_report_to_bq(table_id, gcs_uri)

    print("\n✅ Cluster monitoring report generation complete.")
    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


def test():
    clusters = fetch_clusters()

    for c in clusters:
        cluster_project_id, region, cluster_name = c["project_id"], c["region"], c["cluster_name"]
        project_name = f"projects/{cluster_project_id}"

        print(f"\nFetching cluster {cluster_name} ({cluster_project_id}/{region}) ...")
        cluster_events = fetch_cluster_events(cluster_project_id, cluster_name)


# ================= MAIN =================
if __name__ == "__main__":
    save()
