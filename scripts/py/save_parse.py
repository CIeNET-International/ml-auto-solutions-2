from google.cloud import bigquery, storage
import json
from datetime import datetime, timezone
import uuid
import os
import re

client = bigquery.Client()
gcs_client = storage.Client()

# === Configuration ===
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_prod"
serialized_dag_table = "serialized_dag"
dag_table = "dag"
output_table_1 = "dag_test_info"
output_table_2 = "dag_test_op_kwargs"
gcs_bucket_name = "amy-xlml-poc-prod" 

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
    
def main():
    start_date = datetime.now()
    dag_query = f"SELECT dag_id FROM `{project_id}.{dataset_id}.{dag_table}`"
    dag_ids = [row["dag_id"] for row in client.query(dag_query)]
    print(f"Found {len(dag_ids)} DAGs to process.")
    create_output_tables()
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

    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')

def truncate_table(table_name):
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    query = f"DELETE FROM `{full_table_id}` WHERE TRUE"
    client.query(query).result()
    print(f"Truncated table: {full_table_id}")

if __name__ == "__main__":
    main()
