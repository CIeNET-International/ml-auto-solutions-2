import base64
import tempfile
import time
import uuid
import json
from typing import Dict, Optional, List, Tuple

from kubernetes import client as k8s_client
from google.cloud import bigquery, monitoring_v3, container_v1, storage
from google.auth.transport.requests import Request
from google.auth import default
from google.api_core.exceptions import NotFound
from google.protobuf import timestamp_pb2

import config


# === BigQuery schema for metrics table ===

BQ_SCHEMA = [
    bigquery.SchemaField("project", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("cluster_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("location", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),

    bigquery.SchemaField("pod_health", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("total_pods", "INTEGER"),
        bigquery.SchemaField("unhealthy_pods", "INTEGER"),
        bigquery.SchemaField("crash_loop_backoff", "INTEGER"),
        bigquery.SchemaField("pending_pods", "INTEGER"),
        bigquery.SchemaField("restarting_pods", "INTEGER"),
    ]),

    bigquery.SchemaField("pod_health_per_node", "RECORD", mode="REPEATED", fields=[
        bigquery.SchemaField("node_name", "STRING"),
        bigquery.SchemaField("unhealthy_pods", "INTEGER"),
        bigquery.SchemaField("crash_loop_backoff", "INTEGER"),
        bigquery.SchemaField("pending_pods", "INTEGER"),
        bigquery.SchemaField("restarting_pods", "INTEGER"),
        bigquery.SchemaField("total_pods", "INTEGER"),
    ]),

    bigquery.SchemaField("node_usage", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("cpu_raw", "FLOAT"),
        bigquery.SchemaField("cap_cpu", "FLOAT"),
        bigquery.SchemaField("cpu_percent", "FLOAT"),
        bigquery.SchemaField("mem_raw", "FLOAT"),
        bigquery.SchemaField("cap_mem_bytes", "FLOAT"),
        bigquery.SchemaField("mem_percent", "FLOAT"),
        bigquery.SchemaField("mem_readable", "STRING"),
        bigquery.SchemaField("cap_mem_readable", "STRING"),
        bigquery.SchemaField("node_network_stats", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("node_name", "STRING"),
            bigquery.SchemaField("nodepool", "STRING"),
            bigquery.SchemaField("rx_bytes", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tx_bytes", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("rx_errors", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tx_errors", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("rx_dropped", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("tx_dropped", "FLOAT", mode="NULLABLE"),
        ]),
    ]),

    bigquery.SchemaField("network_metrics", "RECORD", mode="NULLABLE", fields=[
        bigquery.SchemaField("vpc_flows_accept_count", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("vpc_flows_drop_count", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("load_balancer_5xx_errors", "FLOAT", mode="NULLABLE"),
    ]),
]


# --- Helper functions ---

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


def get_pod_health(api_client: k8s_client.ApiClient) -> Dict:
    v1 = k8s_client.CoreV1Api(api_client)
    pods = v1.list_pod_for_all_namespaces(watch=False)
    total_pods = len(pods.items)
    unhealthy = 0
    crash_loop_backoff = 0
    pending = 0
    restarting = 0

    for pod in pods.items:
        status = pod.status
        if status.phase == "Pending":
            pending += 1
        for cs in status.container_statuses or []:
            if cs.state.waiting and cs.state.waiting.reason == "CrashLoopBackOff":
                crash_loop_backoff += 1
            if cs.restart_count and cs.restart_count > 0:
                restarting += 1
            if not cs.ready:
                unhealthy += 1
    return {
        "total_pods": total_pods,
        "unhealthy_pods": unhealthy,
        "crash_loop_backoff": crash_loop_backoff,
        "pending_pods": pending,
        "restarting_pods": restarting,
    }


def get_pod_health_per_node(api_client: k8s_client.ApiClient) -> List[Dict]:
    v1 = k8s_client.CoreV1Api(api_client)
    pods = v1.list_pod_for_all_namespaces(watch=False)

    node_pod_health = {}

    for pod in pods.items:
        node_name = pod.spec.node_name or "unknown"
        if node_name not in node_pod_health:
            node_pod_health[node_name] = {
                "node_name": node_name,
                "unhealthy_pods": 0,
                "crash_loop_backoff": 0,
                "pending_pods": 0,
                "restarting_pods": 0,
                "total_pods": 0,
            }

        node_pod_health[node_name]["total_pods"] += 1
        status = pod.status

        if status.phase == "Pending":
            node_pod_health[node_name]["pending_pods"] += 1
        for cs in status.container_statuses or []:
            if cs.state.waiting and cs.state.waiting.reason == "CrashLoopBackOff":
                node_pod_health[node_name]["crash_loop_backoff"] += 1
            if cs.restart_count and cs.restart_count > 0:
                node_pod_health[node_name]["restarting_pods"] += 1
            if not cs.ready:
                node_pod_health[node_name]["unhealthy_pods"] += 1

    return list(node_pod_health.values())


def get_node_resource_usage(api_client: k8s_client.ApiClient) -> Dict:
    v1 = k8s_client.CoreV1Api(api_client)
    nodes = v1.list_node()
    cpu_used_total = 0
    cpu_capacity_total = 0
    mem_used_total = 0
    mem_capacity_total = 0

    node_network_stats = []

    for node in nodes.items:
        usage = node.status.allocatable
        capacity = node.status.capacity

        cpu_used = parse_quantity(usage.get("cpu"))
        cpu_capacity = parse_quantity(capacity.get("cpu"))
        mem_used = parse_quantity(usage.get("memory"))
        mem_capacity = parse_quantity(capacity.get("memory"))

        if cpu_used:
            cpu_used_total += cpu_used
        if cpu_capacity:
            cpu_capacity_total += cpu_capacity
        if mem_used:
            mem_used_total += mem_used
        if mem_capacity:
            mem_capacity_total += mem_capacity

        node_labels = node.metadata.labels or {}
        nodepool = node_labels.get("cloud.google.com/gke-nodepool", "unknown")

        # Placeholder for network stats (no direct K8s API for those)
        node_network_stats.append({
            "node_name": node.metadata.name,
            "nodepool": nodepool,
            "rx_bytes": None,
            "tx_bytes": None,
            "rx_errors": None,
            "tx_errors": None,
            "rx_dropped": None,
            "tx_dropped": None,
        })

    overall = {
        "cpu_raw": cpu_used_total,
        "cap_cpu": cpu_capacity_total,
        "cpu_percent": calculate_percent(cpu_used_total, cpu_capacity_total),
        "mem_raw": mem_used_total,
        "cap_mem_bytes": mem_capacity_total,
        "mem_percent": calculate_percent(mem_used_total, mem_capacity_total),
        "mem_readable": format_usage(mem_used_total),
        "cap_mem_readable": format_usage(mem_capacity_total),
        "node_network_stats": node_network_stats,
    }

    return overall


def fetch_network_metrics(project_id: str, cluster_name: str) -> Dict:
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

    metrics_to_fetch = {
        "vpc_flows_accept_count": 'logging.googleapis.com/user/vpc_flows_accept_count',
        "vpc_flows_drop_count": 'logging.googleapis.com/user/vpc_flows_drop_count',
        "load_balancer_5xx_errors": 'loadbalancing.googleapis.com/https/backend_latencies',
    }

    result = {}
    for name, metric_type in metrics_to_fetch.items():
        try:
            flt = f'metric.type = "{metric_type}"'
            for ts in mon.list_time_series(
                name=proj,
                filter=flt,
                interval=interval,
                view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                page_size=1,
            ):
                val = ts.points[0].value.double_value or ts.points[0].value.int64_value
                result[name] = val
        except Exception:
            result[name] = None

    return result


def get_cluster_info(project, cluster_name) -> Dict:
    location = get_cluster_location(project, cluster_name)
    if location is None:
        return {
            "project": project,
            "cluster_name": cluster_name,
            "status": "NOT-FOUND",
            "timestamp": int(time.time()),
        }

    api_client = get_k8s_api_client(cluster_name, project, location)
    if api_client is None:
        return {
            "project": project,
            "cluster_name": cluster_name,
            "location": location,
            "status": "NOT-FOUND",
            "timestamp": int(time.time()),
        }

    pod_health = get_pod_health(api_client)
    pod_health_per_node = get_pod_health_per_node(api_client)
    node_usage = get_node_resource_usage(api_client)
    network_metrics = fetch_network_metrics(project, cluster_name)

    return {
        "project": project,
        "cluster_name": cluster_name,
        "location": location,
        "status": "OK",
        "timestamp": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "pod_health": pod_health,
        "pod_health_per_node": pod_health_per_node,
        "node_usage": node_usage,
        "network_metrics": network_metrics,
    }


def write_json_to_gcs(data: List[Dict], bucket: str, prefix: str = "cluster-metrics") -> str:
    client = storage.Client()
    file_name = f"{prefix}/{uuid.uuid4()}.json"
    blob = client.bucket(bucket).blob(file_name)

    json_lines = "\n".join(json.dumps(row) for row in data)
    blob.upload_from_string(json_lines, content_type="application/json")

    gcs_uri = f"gs://{bucket}/{file_name}"
    print(f"✅ Metrics written to GCS: {gcs_uri}")
    return gcs_uri


def load_gcs_to_bq(gcs_uri: str):
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    table_id = f"{config.project_id}.{config.dataset_id}.{config.table_id_metrics_nw}"
    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        job_config=job_config
    )

    load_job.result()
    print(f"✅ Loaded data into BigQuery: {table_id}")


def main():
    projects_clusters = get_clusters_from_view()
    all_data = []

    for project, cluster_name in projects_clusters:
        print(f"Collecting metrics for {project}/{cluster_name}")
        cluster_data = get_cluster_info(project, cluster_name)
        all_data.append(cluster_data)

    if not all_data:
        print("⚠️ No data collected.")
        return

    # Step 1: Write to GCS
    gcs_uri = write_json_to_gcs(all_data, config.bucket_name)

    # Step 2: Load into BigQuery
    load_gcs_to_bq(gcs_uri)

if __name__ == "__main__":
    main()


