import base64
import tempfile
from datetime import datetime
from typing import Optional
from kubernetes import client as k8s_client

from google.cloud import monitoring_v3, container_v1
from google.auth.transport.requests import Request
from google.auth import default
from google.protobuf import timestamp_pb2


def get_k8s_api_client(cluster_name: str, project: str, location: str) -> Optional[k8s_client.ApiClient]:
    container_client = container_v1.ClusterManagerClient()
    cluster_path = f"projects/{project}/locations/{location}/clusters/{cluster_name}"
    try:
        cluster = container_client.get_cluster(name=cluster_path)
    except Exception as e:
        print(f"Error getting cluster info: {e}")
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


def datetime_to_timestamp(dt: datetime) -> timestamp_pb2.Timestamp:
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


def format_metric_value(raw_value: float, unit: str) -> str:
    if unit == "percent":
        return f"{raw_value:.2f}%"
    elif unit == "bytes":
        gb = raw_value / (1024 ** 3)
        return f"{gb:.2f} GB"
    elif unit == "seconds":
        minutes = raw_value / 60
        return f"{minutes:.2f} min"
    elif unit == "cores":
        return f"{raw_value:.2f} cores"
    elif unit == "count":
        return str(int(raw_value))
    else:
        return str(raw_value)

def query_metrics(project: str, cluster_name: str, start_dt: datetime, end_dt: datetime):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project}"

    interval = monitoring_v3.TimeInterval(
        start_time=datetime_to_timestamp(start_dt),
        end_time=datetime_to_timestamp(end_dt),
    )

    #duration_seconds = int((end_dt - start_dt).total_seconds())
    #aggregation = monitoring_v3.Aggregation(
    #    alignment_period={"seconds": duration_seconds},
    #    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    #)
    aggregation = monitoring_v3.Aggregation(
        alignment_period={"seconds": 60},#seems metrics expolorer only sampled every 60 seconds
        per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
    )

    print(f"Query window {start_dt}~{end_dt}")
    metric_filters_old = {
        # GPU/TPU Metrics
        #"gpu_utilization": 'compute.googleapis.com/guest/gpu/utilization',
        #"gpu_memory_utilization": 'compute.googleapis.com/guest/gpu/memory_used_utilization',
        #"gpu_memory_total": 'compute.googleapis.com/guest/gpu/memory_total',
        #"tpu_duty_cycle": 'tpu.googleapis.com/duty_cycle',
        "accelerator_duty_cycle": 'kubernetes.io/container/accelerator/duty_cycle',

        # Node CPU/Memory Metrics (example)
        #"node_cpu_usage": 'compute.googleapis.com/instance/cpu/utilization',
        #"node_memory_usage": 'compute.googleapis.com/instance/memory/usage',

        # Network Metrics
        #"network_received_bytes_count": 'compute.googleapis.com/instance/network/received_bytes_count',
        #"network_sent_bytes_count": 'compute.googleapis.com/instance/network/sent_bytes_count',

        # VPC Flow logs example custom metrics (adjust if you have your own)
        #"vpc_flows_accept_count": 'logging.googleapis.com/user/vpc_flows_accept_count',
        #"vpc_flows_drop_count": 'logging.googleapis.com/user/vpc_flows_drop_count',
        #"load_balancer_5xx_errors": 'loadbalancing.googleapis.com/https/backend_latencies',
    }
    metric_filters = {
        "accelerator_duty_cycle": {
            "metric": "kubernetes.io/container/accelerator/duty_cycle",
            "unit": "percent",
            "aggregate": True
        },
        "accelerator_memory_used": {
            "metric": "kubernetes.io/container/accelerator/memory_used",
            "unit": "bytes",
            "aggregate": True
        },
        "accelerator_memory_total": {
            "metric": "kubernetes.io/container/accelerator/memory_total",
            "unit": "bytes",
            "aggregate": True
        },
        #"container_restart_count": {
        #   "metric": "kubernetes.io/container/restart_count",
        #    "unit": "count",
        #    "aggregate": False
        #},
        #gcloud container clusters update [CLUSTER_NAME] \
        #--region=[LOCATION] \
        #--monitoring=SYSTEM,WORKLOAD \
        #--project=[YOUR_GCP_PROJECT]
        #"pod_status_phase": {
        #    "metric": "kubernetes.io/pod/status/phase",
        #    "unit": "count",
        #    "aggregate": True
        #},
        #"network_received_bytes": {
        #    "metric": "kubernetes.io/container/network/received_bytes_count",
        #    "unit": "bytes",
        #    "aggregate": True
        #},
        #"network_sent_bytes": {
        #    "metric": "kubernetes.io/container/network/sent_bytes_count",
        #    "unit": "bytes,
        #    "aggregate": True"
        #}
    }

    for metric_name, config in metric_filters.items():
        metric_type = config["metric"]
        unit = config["unit"]
        use_aggregation = config.get("aggregate", True)
    #for metric_name, metric_type in metric_filters.items():
        print(f"\n=== Querying metric: {metric_name} ({metric_type})({unit}) ===")
        try:
            request={
                "name": project_name,
                "filter": f'metric.type = "{metric_type}" AND resource.label."cluster_name" = "{cluster_name}"',
                "interval": interval,
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                "page_size": 20,
            }
            if use_aggregation:
                request["aggregation"] = monitoring_v3.Aggregation(
                    alignment_period={"seconds": 60},#seems metrics expolorer only sampled every 60 seconds
                    per_series_aligner=monitoring_v3.Aggregation.Aligner.ALIGN_MEAN
            )

            results = client.list_time_series(request)

            count = 0

            for ts in results:
                count += 1
                print(f"Resource labels: {ts.resource.labels}")
                # Extract labels
                pod_name = ts.resource.labels.get("pod_name", "unknown-pod")
                container_name = ts.resource.labels.get("container_name", "unknown-container")
                zone = ts.resource.labels.get("location", "unknown-zone")
                cluster = ts.resource.labels.get("cluster_name", "unknown-cluster")
                accelerator_id = ts.metric.labels.get("accelerator_id", "unknown-accelerator")
                model = ts.metric.labels.get("model", "unknown-model")
                #thevalue = ts.metric.labels.get("value", "unknown-value")

                print(f"\n📦 {count} Pod: {pod_name}, Accelerator ID: {accelerator_id}, Model:{model}, Zone: {zone}, Container: {container_name}")

                # Time series points
                #for point in ts.points:
                for point in reversed(ts.points):
                    timestamp = point.interval.end_time  # Already a Python datetime
                    #value = point.value.double_value  # This is the duty cycle (0.0 to 100.0)
                    #if value > 0:
                    #print(f"  ⏱️ {timestamp} - 🚀 Duty Cycle: {value:.2f}%")
                    raw_value = point.value.double_value
                    formatted = format_metric_value(raw_value, unit)
                    print(f"{timestamp} {metric_name}: {formatted}")


            #for ts in results:
            #    print(f"Resource labels: {ts.resource.labels}")
            #    for point in ts.points:
            #        print(f"point:{point}")
            #        ts_time = point.interval.end_time.ToDatetime()
            #        val = point.value.double_value if point.value.double_value else point.value.int64_value
            #        print(f"  Timestamp: {ts_time} Value: {val}")
            #    count += 1
            #    if count >= 20:
            #        break
            if count == 0:
                print("  No data points found in this time range.")
        except Exception as e:
            print(f"  Error querying {metric_name}: {e}")


def main():
    # Example clusters list (project, cluster_name, location)
    clusters: List[Tuple[str, str, str]] = [
        #("supercomputer-testing", "ninacai-maxtext-a3", "us-east5"),
        #("cloud-tpu-multipod-dev", "v5p-8-bodaborg-europe-west4-b", "europe-west4"), #prod
        ("tpu-prod-env-one-vm", "bodaborg-v6e-256-lcscld-c", "southamerica-west1"), #prod  maxdiffusion_tpu_e2e maxd-sdxl-jax-stable-stack-v6e-256
        # Add more clusters as needed
    ]


    # Specify UTC time range exactly here:
    #start_time = datetime(2025, 8, 7, 9, 0, 41)
    #end_time = datetime(2025, 8, 7, 9, 10, 31)  
    start_time = datetime(2025, 8, 10, 4, 2, 3)
    end_time = datetime(2025, 8, 10, 4, 45, 21)  

    for project, cluster, location in clusters:
        print(f"\n--- Cluster: {project}/{cluster} ({location}) ---")

        k8s_api = get_k8s_api_client(cluster, project, location)
        if k8s_api is None:
            print("Failed to create Kubernetes API client; skipping cluster.")
            continue

        query_metrics(project, cluster, start_time, end_time)


if __name__ == "__main__":
    main()

