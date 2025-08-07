from google.cloud import bigquery
import json

# Initialize BigQuery client
client = bigquery.Client()

# Replace with your table name
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_2"
table_id = "serialized_dag"
#dag_id_to_query = "mlcompass_maxtext_gke"
dag_id_to_query = "framework_microbenchmark"
#dag_id_to_query = "mxla_maxtext_nightly_gke"




query = f"""
SELECT data
FROM `{project_id}.{dataset_id}.{table_id}`
WHERE dag_id = @dag_id
LIMIT 1
"""

job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id_to_query)
    ]
)

query_job = client.query(query, job_config=job_config)
rows = list(query_job)

if not rows:
    print("No DAG found with that dag_id.")
    exit()

row = rows[0]
serialized_data = row["data"]
dag_data = json.loads(serialized_data)

tasks = dag_data.get("dag", {}).get("tasks", [])
print(f"Found {len(tasks)} tasks.")

# Add "region" to the list
required_op_kwargs = [
    "task_id",
    "cluster_project",
    "zone",
    "cluster_name",
    "accelerator_type",
    "num_slices",
    "region"
]

def strip_quotes(s: str) -> str:
    """Remove matching quotes from start/end of string if present."""
    s = s.strip()
    if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
        return s[1:-1]
    return s

def parse_op_kwargs_string(op_str: str) -> dict:
    """
    Parses op_kwargs string of format: 
    "'key1': 'val1', key2: val2, key3: 'val3'"
    Supports quoted and unquoted values/keys.
    """
    result = {}

    op_str = op_str.strip()

    # Remove enclosing braces if present
    if op_str.startswith("{") and op_str.endswith("}"):
        op_str = op_str[1:-1].strip()

    pairs = op_str.split(",")
    print(f"Parsing op_kwargs string: {op_str}")
    for i, pair in enumerate(pairs):
        pair = pair.strip()
        if ":" in pair:
            key, value = pair.split(":", 1)
            key = strip_quotes(key.strip())
            value = strip_quotes(value.strip())
            print(f"  Pair {i}: key='{key}', value='{value}'")
            result[key] = value
        else:
            print(f"  Pair {i} malformed (no colon): '{pair}'")
    print(f"  Parsed op_kwargs dict: {result}")
    return result

# Superset-style merge
superset_tasks = {}

for idx, task in enumerate(tasks):
    var_block = task.get("__var")

    if isinstance(var_block, str):
        try:
            var_block = json.loads(var_block)
            print(f"Task [{idx}]: Parsed __var string to dict")
        except json.JSONDecodeError:
            print(f"Task [{idx}]: __var string not JSON, skipping task.")
            continue

    if not isinstance(var_block, dict):
        print(f"Task [{idx}]: __var is not a dict, skipping task.")
        continue

    full_task_id = var_block.get("task_id")
    if not full_task_id:
        print(f"Task [{idx}]: No task_id found, skipping task.")
        continue

    short_task_id = full_task_id.split(".")[0]

    op_kwargs = var_block.get("op_kwargs")
    print(f"Task [{idx}] '{short_task_id}': original op_kwargs = {op_kwargs}")

    if isinstance(op_kwargs, str):
        op_kwargs = parse_op_kwargs_string(op_kwargs)

    if not isinstance(op_kwargs, dict):
        print(f"Task [{idx}] '{short_task_id}': op_kwargs not dict after parsing, skipping.")
        continue

    # Initialize if not exists
    if short_task_id not in superset_tasks:
        superset_tasks[short_task_id] = {"task_id": short_task_id}

    merged = superset_tasks[short_task_id]

    for key in required_op_kwargs:
        if key == "task_id":
            continue
        value = op_kwargs.get(key)
        if value and not merged.get(key):
            merged[key] = value
            print(f"  Task [{idx}] '{short_task_id}': added {key} = {value}")

# Final filter: must have non-empty cluster_name
final_results = {
    k: v for k, v in superset_tasks.items() if v.get("cluster_name")
}

if final_results:
    print("\n==== Extracted Unique Superset Tasks with non-empty cluster_name ====")
    for task_id, info in final_results.items():
        print(info)
else:
    print("No tasks with valid cluster_name found.")

