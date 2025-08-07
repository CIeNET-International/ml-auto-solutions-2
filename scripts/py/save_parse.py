from google.cloud import bigquery
import json
from datetime import datetime, timezone

client = bigquery.Client()

# === Configuration ===
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_2"
serialized_dag_table = "serialized_dag"
dag_table = "dag"

output_table_1 = "dag_test_info"
output_table_2 = "dag_test_op_kwargs"

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

def truncate_table(table_name):
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    query = f"DELETE FROM `{full_table_id}` WHERE TRUE"
    client.query(query).result()
    print(f"Truncated table: {full_table_id}")

# === Main Execution ===

dag_query = f"SELECT dag_id FROM `{project_id}.{dataset_id}.{dag_table}`"
dag_ids = [row["dag_id"] for row in client.query(dag_query)]

print(f"Found {len(dag_ids)} DAGs to process.")

create_output_tables()
truncate_table(output_table_1)
truncate_table(output_table_2)

summary_rows = []
kv_rows = []
load_time = datetime.now(timezone.utc).isoformat()
total_dags = len(dag_ids)

for index, dag_id in enumerate(dag_ids, start=1):
    print(f"Processing DAG {index}/{total_dags}: {dag_id}")
    query = f"""
        SELECT data
        FROM `{project_id}.{dataset_id}.{serialized_dag_table}`
        WHERE dag_id = @dag_id
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id)]
    )
    result = list(client.query(query, job_config=job_config))
    if not result:
        print(f"No serialized DAG found for {dag_id}")
        continue
    try:
        dag_data = json.loads(result[0]["data"])
    except Exception as e:
        print(f"Failed to parse DAG {dag_id}: {e}")
        continue
    tasks = dag_data.get("dag", {}).get("tasks", [])
    merged_tasks = merge_tasks(tasks)
    for test_id, info in merged_tasks.items():
        row = {
            "dag_id": dag_id,
            "test_id": info.get("test_id"),
            "cluster_project": info.get("cluster_project"),
            "cluster_name": info.get("cluster_name"),
            "region": info.get("region"),
            "zone": info.get("zone"),
            "accelerator_type": info.get("accelerator_type"),
            "num_slices": None,  # Initialize num_slices to None
            "load_time": load_time,
        }
        num_slices_str = info.get("num_slices")
        if num_slices_str:  
            try:
                num_slices_int = int(num_slices_str) 
                row["num_slices"] = num_slices_int 
            except (ValueError, TypeError):
                # If the conversion fails (not an integer), leave num_slices as None
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

if summary_rows:
    client.insert_rows_json(f"{project_id}.{dataset_id}.{output_table_1}", summary_rows)
    print(f"Inserted {len(summary_rows)} rows into {output_table_1}")
else:
    print("No summary data to insert.")

if kv_rows:
    client.insert_rows_json(f"{project_id}.{dataset_id}.{output_table_2}", kv_rows)
    print(f"Inserted {len(kv_rows)} rows into {output_table_2}")
else:
    print("No key-value data to insert.")

