from google.cloud import bigquery, storage
import json
from datetime import datetime, timezone
import uuid
import os
import re
import collections

from dags.dashboard.statistic.config import (
    BQ_PROJECT_ID, BQ_DATASET,
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
)

project_id = BQ_PROJECT_ID
dataset_id = BQ_DATASET
gcs_bucket_name = GCS_BUCKET_NAME
client = bigquery.Client()
gcs_client = storage.Client()

# === Configuration ===
serialized_dag_table = "serialized_dag"
#dag_table = "dag"
dag_table = "base"
output_table_1 = "dag_test_info"
output_table_2 = "dag_test_op_kwargs"
output_table_3 = "dag_task_order"
output_table_4 = "dag_tasks"


client = bigquery.Client()
gcs_client = storage.Client()

required_op_kwargs = [
    "test_id",
    "cluster_project",
    "cluster_name",
    "region",
    "zone",
    "accelerator_type",
    "num_slices",
]

def strip_quotes(s: str) -> str:
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    return s

def parse_op_kwargs_string(op_str: str) -> dict:
    result = {}
    op_str = op_str.strip()
    if op_str.startswith("{") and op_str.endswith("}"):
        op_str = op_str[1:-1].strip()
    pairs = op_str.split(",")
    for pair in pairs:
        pair = pair.strip()
        if ":" in pair:
            key, value = pair.split(":", 1)
        elif "=" in pair:
            key, value = pair.split("=", 1)
        else:
            key = strip_quotes(pair)
            result[key] = None
            continue
        key = strip_quotes(key.strip())
        value = strip_quotes(value.strip())
        result[key] = value if key else None
    return result

def merge_tasks(tasks):
    superset_tasks = {}
    for task in tasks:
        var_block = task.get("__var")
        if isinstance(var_block, str):
            try:
                var_block = json.loads(var_block)
            except json.JSONDecodeError:
                continue
        if not isinstance(var_block, dict):
            continue
        full_task_id = var_block.get("task_id")
        if not full_task_id:
            continue
        short_task_id = full_task_id.split(".")[0]
        op_kwargs = var_block.get("op_kwargs")
        if isinstance(op_kwargs, str):
            op_kwargs = parse_op_kwargs_string(op_kwargs)
        if not isinstance(op_kwargs, dict):
            continue
        if short_task_id not in superset_tasks:
            superset_tasks[short_task_id] = {"test_id": short_task_id}
        merged = superset_tasks[short_task_id]
        for key, value in op_kwargs.items():
            if key and value and not merged.get(key):
                merged[key] = value
    return {k: v for k, v in superset_tasks.items() if v.get("cluster_name")}

def create_table_if_not_exists(table_ref, schema):
    try:
        client.get_table(table_ref)
        print(f"Table exists: {table_ref}")
    except Exception:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table: {table_ref}")

def create_output_tables():
    table1_ref = f"{project_id}.{dataset_id}.{output_table_1}"
    schema1 = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("test_id", "STRING"),
        bigquery.SchemaField("cluster_project", "STRING"),
        bigquery.SchemaField("cluster_name", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("accelerator_type", "STRING"),
        bigquery.SchemaField("accelerator_family", "STRING"),
        bigquery.SchemaField("num_slices", "INT64"),
        bigquery.SchemaField("load_time", "TIMESTAMP"),
    ]
    create_table_if_not_exists(table1_ref, schema1)

    table2_ref = f"{project_id}.{dataset_id}.{output_table_2}"
    schema2 = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("test_id", "STRING"),
        bigquery.SchemaField("key", "STRING"),
        bigquery.SchemaField("value", "STRING"),
        bigquery.SchemaField("load_time", "TIMESTAMP"),
    ]
    create_table_if_not_exists(table2_ref, schema2)

    table3_ref = f"{project_id}.{dataset_id}.{output_table_3}"
    schema3 = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("task_order", "STRUCT", mode="REPEATED", fields=[
            bigquery.SchemaField("task_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("disp_order", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("parents", "STRING", mode="REPEATED"),
            bigquery.SchemaField("type", "STRING", mode="REQUIRED"),
        ]),

    ]
    create_table_if_not_exists(table3_ref, schema3)

    table4_ref = f"{project_id}.{dataset_id}.{output_table_4}"
    schema4 = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("tasks", "STRING", mode="REPEATED"),
    ]
    create_table_if_not_exists(table4_ref, schema4)


def upload_to_gcs(bucket_name, file_path, gcs_path):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)
    print(f"Uploaded {file_path} to gs://{bucket_name}/{gcs_path}")
    
def load_from_gcs_and_truncate(table_id, gcs_uri):
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {gcs_uri} into {table_id} with TRUNCATE.")

def categorize_op_kwargs(value: str) -> str:
    """
    Categorize accelerator string (from op_kwargs) into TPU, GPU, or CPU.
    """
    v = value.lower()

    # TPU rules (ct*, explicit "tpu", or any v[0-9] family reference)
    if re.search(r'(^ct|tpu|v[0-9]+)', v):
        return "TPU"

    # GPU rules (NVIDIA family names, accelerator models)
    elif re.search(r'(nvidia|gpu|a100|h100|t4|v100|k80|l4|l40|l40s|p4|p100|h200)', v):
        return "GPU"

    # Default: CPU
    else:
        return "CPU"    
    

def build_display_order_and_parents(dag_id, serialized_dag_data):
    """
    Builds a list of tasks with their display order and parent hierarchy
    by using a topological sort on task dependencies.
    """
    task_list = []
    task_payload_map = {}
    task_in_var = []

    # Create a lookup map for task details
    for task_info in serialized_dag_data.get('dag', {}).get('tasks', []):
        task_payload = task_info.get('__var', {})
        task_id = task_payload.get('task_id')
        if task_id:
            task_payload_map[task_id] = task_payload
            task_in_var.append(task_id)

    # Create the dependency graph and in-degree map for topological sort
    adj_list = collections.defaultdict(list)
    in_degree = collections.defaultdict(int)

    def _build_graph(node, parent_id=''):
        node_type, payload = node

        if node_type == 'taskgroup':
            group_id = payload.get('_group_id')
            full_id = f'{parent_id}.{group_id}' if parent_id else group_id

            for child_key, child_data in payload.get('children', {}).items():
                _build_graph(child_data, parent_id=full_id)

            # Group-level dependencies
            for down_task in payload.get('downstream_task_ids', []):
                adj_list[full_id].append(down_task)
                in_degree[down_task] += 1
            for up_task in payload.get('upstream_task_ids', []):
                adj_list[up_task].append(full_id)
                in_degree[full_id] += 1

        elif node_type == 'operator':
            task_id = payload
            task_payload = task_payload_map.get(task_id)
            if not task_payload:
                return

            for down_task in task_payload.get('downstream_task_ids', []):
                adj_list[task_id].append(down_task)
                in_degree[down_task] += 1
            for up_task in task_payload.get('upstream_task_ids', []):
                adj_list[up_task].append(task_id)
                in_degree[task_id] += 1

    root_children = serialized_dag_data.get('dag', {}).get('_task_group', {}).get('children', {})
    for _, data in root_children.items():
        _build_graph(data)

    # Perform topological sort
    queue = collections.deque([node for node in adj_list if in_degree[node] == 0])
    ordered_ids = []
    while queue:
        node = queue.popleft()
        ordered_ids.append(node)
        for neighbor in sorted(adj_list[node]):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Build the final list with display order and parents
    display_order = 1
    task_paths = {}

    # Get all task IDs and group IDs from the serialized data
    all_ids = set(task_payload_map.keys())

    def _collect_ids(children_dict, parent_path):
        for child_key, child_data in children_dict.items():
            child_type, child_payload = child_data
            if child_type == "taskgroup":
                group_id = child_payload.get("_group_id")
                new_parent_path = parent_path + [group_id]
                all_ids.add(group_id)
                _collect_ids(child_payload.get("children", {}), new_parent_path)
            elif child_type == "operator":
                task_id = child_payload
                task_paths[task_id] = parent_path

    root_children = serialized_dag_data.get('dag', {}).get('_task_group', {}).get('children', {})
    _collect_ids(root_children, [])

    # Now, assign display order based on topological sort
    for task_id in ordered_ids:
        if task_id in all_ids:
            parent_path = task_paths.get(task_id, [])
            task_list.append({
                "task_id": task_id,
                "disp_order": display_order,
                "parents": parent_path,
                "type": "task"
            })
            display_order += 1
            all_ids.remove(task_id)

    # Add any remaining nodes that were not part of the main flow
    for task_id in sorted(list(all_ids)):
        parent_path = task_paths.get(task_id, [])
        task_list.append({
            "task_id": task_id,
            "disp_order": display_order,
            "parents": parent_path,
            "type": "group"
        })
        display_order += 1

    return task_list, task_in_var


def save():
    start_date = datetime.now()
    dag_query = f"SELECT dag_id,data FROM `{project_id}.{dataset_id}.{serialized_dag_table}`"

    client = bigquery.Client(project=project_id)

    rows = list(client.query(dag_query).result())

    total_dags = len(rows)

    print(f"Found {total_dags} DAGs to process.")
    create_output_tables()
    summary_rows = []
    kv_rows = []
    task_order_rows = []
    task_list_rows = []
    load_time = datetime.now(timezone.utc).isoformat()

    for index, row in enumerate(rows, start=1):
        dag_id = row["dag_id"]
        print(f"Processing DAG {index}/{total_dags}: {dag_id}")
        try:
            dag_data = json.loads(row["data"])
        except Exception as e:
            print(f"Failed to parse DAG {dag_id}: {e}")
            continue

        ordered_tasks_data, task_in_var = build_display_order_and_parents(dag_id, dag_data)
        order_row_data = {
            "dag_id": dag_id,
            "task_order": ordered_tasks_data
        }
        task_order_rows.append(order_row_data)
        task_row_data = {
            "dag_id": dag_id,
            "tasks": task_in_var
        }
        task_list_rows.append(task_row_data)


        tasks = dag_data.get("dag", {}).get("tasks", [])
        merged_tasks = merge_tasks(tasks)
        for test_id, info in merged_tasks.items():
            accelerator_type = info.get("accelerator_type")
            acceleratoe_family = None
            if accelerator_type:
                accelerator_family = categorize_op_kwargs(accelerator_type)
            row = {
                "dag_id": dag_id,
                "test_id": info.get("test_id"),
                "cluster_project": info.get("cluster_project"),
                "cluster_name": info.get("cluster_name"),
                "region": info.get("region"),
                "zone": info.get("zone"),
                "accelerator_type": info.get("accelerator_type"),
                "accelerator_family": accelerator_family,
                "num_slices": None,
                "load_time": load_time,
            }
            num_slices_str = info.get("num_slices")
            if num_slices_str:
                try:
                    num_slices_int = int(num_slices_str)
                    row["num_slices"] = num_slices_int
                except (ValueError, TypeError):
                    print(f"Warning: Could not convert '{num_slices_str}' to an integer for dag_id: {info.get('dag_id')}, test_id: {info.get('test_id')}. Setting num_slices to None.")
            summary_rows.append(row)
        kv_seen = {}
        for test_id, info in merged_tasks.items():
            for key, value in info.items():
                if key != "test_id":
                    dedup_key = (dag_id, test_id, key)
                    if dedup_key not in kv_seen:
                        kv_seen[dedup_key] = value
        for (dag_id_val, test_id_val, key), value in kv_seen.items():
            kv_rows.append({
                "dag_id": dag_id_val,
                "test_id": test_id_val,
                "key": key,
                "value": value,
                "load_time": load_time
            })
    
    # Write to local files
    summary_file_path = f"summary_rows_{uuid.uuid4()}.jsonl"
    kv_file_path = f"kv_rows_{uuid.uuid4()}.jsonl"
    task_order_file_path = f"taskorder_rows_{uuid.uuid4()}.jsonl"
    task_list_file_path = f"tasklist_rows_{uuid.uuid4()}.jsonl"
    
    if summary_rows:
        with open(summary_file_path, 'w') as f:
            for row in summary_rows:
                f.write(json.dumps(row) + '\n')
        print(f"Wrote {len(summary_rows)} rows to local file {summary_file_path}")
    
    if kv_rows:
        with open(kv_file_path, 'w') as f:
            for row in kv_rows:
                f.write(json.dumps(row) + '\n')
        print(f"Wrote {len(kv_rows)} rows to local file {kv_file_path}")
        
    if task_order_rows:
        with open(task_order_file_path, 'w') as f:
            for row in task_order_rows:
                f.write(json.dumps(row) + '\n')
        print(f"Wrote {len(task_order_rows)} rows to local file {task_order_file_path}")
    if task_list_rows:
        with open(task_list_file_path, 'w') as f:
            for row in task_list_rows:
                f.write(json.dumps(row) + '\n')
        print(f"Wrote {len(task_list_rows)} rows to local file {task_list_file_path}")
        
    # Upload to GCS and load into BigQuery
    if summary_rows:
        gcs_path_1 = f"bq_load/{output_table_1}/{os.path.basename(summary_file_path)}"
        upload_to_gcs(gcs_bucket_name, summary_file_path, gcs_path_1)
        load_from_gcs_and_truncate(f"{project_id}.{dataset_id}.{output_table_1}", f"gs://{gcs_bucket_name}/{gcs_path_1}")
        os.remove(summary_file_path)
    else:
        print("No summary data to load. Truncating table anyway.")
        truncate_table(output_table_1)
        
    if kv_rows:
        gcs_path_2 = f"bq_load/{output_table_2}/{os.path.basename(kv_file_path)}"
        upload_to_gcs(gcs_bucket_name, kv_file_path, gcs_path_2)
        load_from_gcs_and_truncate(f"{project_id}.{dataset_id}.{output_table_2}", f"gs://{gcs_bucket_name}/{gcs_path_2}")
        os.remove(kv_file_path)
    else:
        print("No key-value data to load. Truncating table anyway.")
        truncate_table(output_table_2)

    if task_order_rows:
        gcs_path_3 = f"bq_load/{output_table_3}/{os.path.basename(task_order_file_path)}"
        upload_to_gcs(gcs_bucket_name, task_order_file_path, gcs_path_3)
        load_from_gcs_and_truncate(f"{project_id}.{dataset_id}.{output_table_3}", f"gs://{gcs_bucket_name}/{gcs_path_3}")
        os.remove(task_order_file_path)
    else:
        print("No key-value data to load. Truncating table anyway.")
        truncate_table(output_table_3)
    if task_list_rows:
        gcs_path_4 = f"bq_load/{output_table_4}/{os.path.basename(task_list_file_path)}"
        upload_to_gcs(gcs_bucket_name, task_list_file_path, gcs_path_4)
        load_from_gcs_and_truncate(f"{project_id}.{dataset_id}.{output_table_4}", f"gs://{gcs_bucket_name}/{gcs_path_4}")
        os.remove(task_list_file_path)
    else:
        print("No key-value data to load. Truncating table anyway.")
        truncate_table(output_table_4)

    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')

def truncate_table(table_name):
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    query = f"DELETE FROM `{full_table_id}` WHERE TRUE"
    client.query(query).result()
    print(f"Truncated table: {full_table_id}")

if __name__ == "__main__":
    save()

