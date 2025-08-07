import base64
import tempfile
import time
import uuid
import json
import re
from datetime import datetime
from typing import Dict, Optional, List, Tuple

from kubernetes import client as k8s_client
from google.cloud import bigquery, monitoring_v3, container_v1, storage
from google.auth.transport.requests import Request
from google.auth import default
from google.api_core.exceptions import NotFound
from google.protobuf import timestamp_pb2

import config


# -------------- Utility Functions ----------------

def sanitize_key(key: str) -> str:
    key = re.sub(r'[^A-Za-z0-9_]', '_', key)
    key = re.sub(r'_+', '_', key).strip('_')
    if not re.match(r'^[A-Za-z_]', key):
        key = f"f_{key}"
    return key

def sanitize_dict_keys(d):
    if isinstance(d, dict):
        return {sanitize_key(k): sanitize_dict_keys(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [sanitize_dict_keys(i) for i in d]
    else:
        return d

def parse_quantity(quantity: str) -> Optional[float]:
    if quantity is None:
        return None
    try:
        if quantity.endswith('n'):
            return int(quantity[:-1]) / 1e9
        elif quantity.endswith('u'):
            return int(quantity[:-1]) / 1e6
        elif quantity.endswith('m'):
            return int(quantity[:-1]) / 1000.0
        elif quantity.endswith('Ki'):
            return int(quantity[:-2]) * 1024
        elif quantity.endswith('Mi'):
            return int(quantity[:-2]) * 1024 * 1024
        elif quantity.endswith('Gi'):
            return int(quantity[:-2]) * 1024 * 1024 * 1024
        elif quantity.endswith('Ti'):
            return int(quantity[:-2]) * 1024 ** 4
        elif quantity[-1].isdigit():
            return float(quantity)
        else:
            suffixes = {"K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12}
            for suf, mult in suffixes.items():
                if quantity.endswith(suf):
                    return float(quantity[:-1]) * mult
            return float(quantity)
    except Exception:
        return None

def format_usage(bytes_val: Optional[float]) -> Optional[str]:
    if bytes_val is None:
        return None
    for unit in ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']:
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} EiB"

def calculate_percent(used: Optional[float], total: Optional[float]) -> Optional[float]:
    if used is None or total is None or total == 0:
        return None
    return (used / total) * 100

# -------------- Kubernetes and GCP Functions ----------------

def get_clusters_from_view() -> List[Tuple[str, str]]:
    client = bigquery.Client()
    query = f"""
        SELECT project_name, cluster_name
        FROM `{config.project_id}.{config.dataset_id}.{config.view_name}`
    """
    results = client.query(query).result()
    return [(row.project_name, row.cluster_name) for row in results]

def get_cluster_location(project: str, cluster_name: str) -> Optional[str]:
    client = container_v1.ClusterManagerClient()
    parent = f"projects/{project}/locations/-"
    try:
        response = client.list_clusters(parent=parent)
        for cluster in response.clusters:
            if cluster.name == cluster_name:
                return cluster.location
    except Exception:
        return None
    return None

def get_k8s_api_client(cluster_name: str, project: str, location: str) -> Optional[k8s_client.ApiClient]:
    container_client = container_v1.ClusterManagerClient()
    cluster_path = f"projects/{project}/locations/{location}/clusters/{cluster_name}"
    try:
        cluster = container_client.get_cluster(name=cluster_path)
    except NotFound:
        return None

    ca_cert = cluster.master_auth.cluster_ca_certificate
    endpoint = cluster.endpoint

    configuration = k8s_client.Configuration()
    configuration.host = f"https://{endpoint}"
    ca_cert_bytes = base64.b64decode(ca_cert)
    with tempfile.NamedTemporaryFile(delete=False) as ca_file:
        ca_file.write(ca_cert_bytes)
        ca_path = ca_file.name
    configuration.ssl_ca_cert = ca_path
    configuration.verify_ssl = True

    creds, _ = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    creds.refresh(Request())
    configuration.api_key = {"authorization": "Bearer " + creds.token}
    return k8s_client.ApiClient(configuration)

# -------------- Cluster Inspection Functions ----------------

def get_pod_health(api_client: k8s_client.ApiClient) -> Tuple[Dict, Dict[str, Dict]]:
    v1 = k8s_client.CoreV1Api(api_client)
    pods = v1.list_pod_for_all_namespaces(watch=False)
    summary = {
        "total_pods": 0,
        "unhealthy_pods": 0,
        "crash_loop_backoff": 0,
        "pending_pods": 0,
        "restarting_pods": 0
    }
    per_node = {}

    for pod in pods.items:
        node_name = pod.spec.node_name or "unscheduled"
        if node_name not in per_node:
            per_node[node_name] = {
                "node_total_pods": 0,
                "node_unhealthy_pods": 0,
                "node_crash_loop_backoff": 0,
                "node_pending_pods": 0,
                "node_restarting_pods": 0
            }

        summary["total_pods"] += 1
        per_node[node_name]["node_total_pods"] += 1

        if pod.status.phase == "Pending":
            summary["pending_pods"] += 1
            per_node[node_name]["node_pending_pods"] += 1

        for cs in pod.status.container_statuses or []:
            if cs.state.waiting and cs.state.waiting.reason == "CrashLoopBackOff":
                summary["crash_loop_backoff"] += 1
                per_node[node_name]["node_crash_loop_backoff"] += 1
            if cs.restart_count and cs.restart_count > 0:
                summary["restarting_pods"] += 1
                per_node[node_name]["node_restarting_pods"] += 1
            if not cs.ready:
                summary["unhealthy_pods"] += 1
                per_node[node_name]["node_unhealthy_pods"] += 1

    return summary, per_node

def get_cluster_info(project, cluster_name) -> Dict:
    location = get_cluster_location(project, cluster_name)
    if location is None:
        return {"project": project, "cluster_name": cluster_name, "status": "NOT-FOUND"}

    api_client = get_k8s_api_client(cluster_name, project, location)
    if api_client is None:
        return {"project": project, "cluster_name": cluster_name, "location": location, "status": "NOT-FOUND"}

    v1 = k8s_client.CoreV1Api(api_client)
    nodes = v1.list_node()
    pod_summary, pod_breakdown = get_pod_health(api_client)

    node_infos = []
    for node in nodes.items:
        name = node.metadata.name
        labels = json.dumps(node.metadata.labels)
        status = "Ready" if any(c.type == "Ready" and c.status == "True" for c in node.status.conditions) else "NotReady"
        node_pool = node.metadata.labels.get("cloud.google.com/gke-nodepool", "unknown")

        alloc = node.status.allocatable
        cap = node.status.capacity

        cpu_alloc = parse_quantity(alloc.get("cpu"))
        cpu_cap = parse_quantity(cap.get("cpu"))
        mem_alloc = parse_quantity(alloc.get("memory"))
        mem_cap = parse_quantity(cap.get("memory"))

        node_infos.append({
            "node_name": name,
            "node_pool": node_pool,
            "node_status": status,
            "labels": labels,
            "cpu_allocatable": cpu_alloc,
            "cpu_capacity": cpu_cap,
            "cpu_percent": calculate_percent(cpu_alloc, cpu_cap),
            "mem_allocatable": mem_alloc,
            "mem_capacity": mem_cap,
            "mem_percent": calculate_percent(mem_alloc, mem_cap),
            "mem_allocatable_readable": format_usage(mem_alloc),
            "mem_capacity_readable": format_usage(mem_cap),
            **pod_breakdown.get(name, {
                "node_total_pods": 0,
                "node_unhealthy_pods": 0,
                "node_crash_loop_backoff": 0,
                "node_pending_pods": 0,
                "node_restarting_pods": 0
            })
        })

    return sanitize_dict_keys({
        "project": project,
        "cluster_name": cluster_name,
        "location": location,
        "status": "OK",
        "timestamp": datetime.utcnow().isoformat(),
        **pod_summary,
        "nodes": node_infos,
        **fetch_gpu_metrics(project, cluster_name),
        **fetch_tpu_metrics(project, cluster_name),
    })

# -------------- Monitoring (GPU/TPU) ----------------

def fetch_gpu_metrics(project_id: str, cluster_name: str) -> Dict:
    mon = monitoring_v3.MetricServiceClient()
    proj = f"projects/{project_id}"

    interval = monitoring_v3.TimeInterval()
    now = time.time()
    start_timestamp = timestamp_pb2.Timestamp()
    end_timestamp = timestamp_pb2.Timestamp()
    start_timestamp.FromSeconds(int(now - 300))
    end_timestamp.FromSeconds(int(now))
    interval.start_time = start_timestamp
    interval.end_time = end_timestamp


    metric_map = {
        "compute.googleapis.com/guest/gpu/utilization": "gpu_utilization",
        "compute.googleapis.com/guest/gpu/memory_used_bytes": "gpu_memory_used_bytes",
        "compute.googleapis.com/guest/gpu/memory_total_bytes": "gpu_memory_total_bytes"
    }

    result = {}
    for metric_type, field_name in metric_map.items():
        try:
            flt = f'metric.type = "{metric_type}"'
            for ts in mon.list_time_series(
                name=proj,
                filter=flt,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                page_size=1,
            ):
                if ts.resource.labels.get("cluster_name") == cluster_name:
                    val = ts.points[0].value.double_value or ts.points[0].value.int64_value
                    result[field_name] = val
        except Exception:
            result[field_name] = None
    return result

def fetch_tpu_metrics(project_id: str, cluster_name: str) -> Dict:
    mon = monitoring_v3.MetricServiceClient()
    proj = f"projects/{project_id}"

    interval = monitoring_v3.TimeInterval()
    now = time.time()
    start_timestamp = timestamp_pb2.Timestamp()
    end_timestamp = timestamp_pb2.Timestamp()
    start_timestamp.FromSeconds(int(now - 300))
    end_timestamp.FromSeconds(int(now))
    interval.start_time = start_timestamp
    interval.end_time = end_timestamp


    metric_map = {
        "tpu.googleapis.com/accelerator/duty_cycle": "tpu_duty_cycle",
        "tpu.googleapis.com/accelerator/duty_cycle_per_core": "tpu_duty_cycle_per_core"
    }

    result = {}
    for metric_type, field_name in metric_map.items():
        try:
            flt = f'metric.type = "{metric_type}"'
            for ts in mon.list_time_series(
                name=proj,
                filter=flt,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                page_size=1,
            ):
                if ts.resource.labels.get("cluster_name") == cluster_name:
                    val = ts.points[0].value.double_value or ts.points[0].value.int64_value
                    result[field_name] = val
        except Exception:
            result[field_name] = None
    return result

# -------------- GCS + BigQuery ----------------

def write_to_gcs(data: list, bucket_name: str, prefix: str = "cluster-metrics") -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    file_name = f"{prefix}/{uuid.uuid4()}.json"
    blob = bucket.blob(file_name)
    blob.upload_from_string("\n".join([json.dumps(row) for row in data]), content_type="application/json")
    return f"gs://{bucket_name}/{file_name}"

def get_bigquery_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("project", "STRING"),
        bigquery.SchemaField("cluster_name", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("total_pods", "INTEGER"),
        bigquery.SchemaField("unhealthy_pods", "INTEGER"),
        bigquery.SchemaField("crash_loop_backoff", "INTEGER"),
        bigquery.SchemaField("pending_pods", "INTEGER"),
        bigquery.SchemaField("restarting_pods", "INTEGER"),
        bigquery.SchemaField("gpu_utilization", "FLOAT"),
        bigquery.SchemaField("gpu_memory_used_bytes", "FLOAT"),
        bigquery.SchemaField("gpu_memory_total_bytes", "FLOAT"),
        bigquery.SchemaField("tpu_duty_cycle", "FLOAT"),
        bigquery.SchemaField("tpu_duty_cycle_per_core", "FLOAT"),
        bigquery.SchemaField("nodes", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("node_name", "STRING"),
            bigquery.SchemaField("node_pool", "STRING"),
            bigquery.SchemaField("node_status", "STRING"),
            bigquery.SchemaField("labels", "STRING"),
            bigquery.SchemaField("cpu_allocatable", "FLOAT"),
            bigquery.SchemaField("cpu_capacity", "FLOAT"),
            bigquery.SchemaField("cpu_percent", "FLOAT"),
            bigquery.SchemaField("mem_allocatable", "FLOAT"),
            bigquery.SchemaField("mem_capacity", "FLOAT"),
            bigquery.SchemaField("mem_percent", "FLOAT"),
            bigquery.SchemaField("mem_allocatable_readable", "STRING"),
            bigquery.SchemaField("mem_capacity_readable", "STRING"),
            bigquery.SchemaField("node_total_pods", "INTEGER"),
            bigquery.SchemaField("node_unhealthy_pods", "INTEGER"),
            bigquery.SchemaField("node_crash_loop_backoff", "INTEGER"),
            bigquery.SchemaField("node_pending_pods", "INTEGER"),
            bigquery.SchemaField("node_restarting_pods", "INTEGER"),
        ])
    ]

def load_gcs_to_bigquery(gcs_uri: str):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        schema=get_bigquery_schema(),
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    table_ref = f"{config.project_id}.{config.dataset_id}.{config.table_id_metrics}"
    job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    job.result()

# -------------- Main ----------------

def main():
    all_data = []
    for project, cluster_name in get_clusters_from_view():
        print(f"Processing {project}/{cluster_name}")
        info = get_cluster_info(project, cluster_name)
        all_data.append(info)

    gcs_uri = write_to_gcs(all_data, config.bucket_name)
    load_gcs_to_bigquery(gcs_uri)

if __name__ == "__main__":
    main()

