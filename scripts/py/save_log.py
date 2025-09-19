import re
import json
from google.protobuf.json_format import MessageToDict, MessageToJson
import urllib.parse
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage, logging_v2
from urllib.parse import quote
from config_log import (
    BQ_PROJECT_ID, BQ_DATASET, BQ_VIEW_NAME, BQ_DEST_TABLE,
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
    LOGS_PROJECT_ID, LOG_EXPLORER_HOST,
    LOG_QUERY_END_PADDING_SECONDS, LOG_QUERY_SEVERITY,
    HACKED_DAG_TIMES, DAGS_TO_QUERY_LOGS
)
import save_log_utils as utils
import log_explorer_err_sentence as err_sentence


def get_airflow_logs(dag_id, run_id, test_id, test_start_date, end_with_padding, severity):
    """Query and format Airflow logs."""
    #filter_str_error = utils.build_log_filter(
    #    dag_id, run_id, test_id, 
    #    test_start_date, end_with_padding,
    #    severity=severity
    #)
    filter_str_error = utils.build_log_filter_errors_plus_keywords(
        dag_id, run_id, test_id, 
        test_start_date, end_with_padding
    )
    entries = utils.query_logs(filter_str_error, LOGS_PROJECT_ID, 500)
    logs_entries = []
    logs_entries_keywords = []
    messages_keywords = set()
    error_messages = set()
    for e in reversed(entries):
      severity = getattr(e, "severity", "DEFAULT")
      if severity in ['ERROR', 'CRITICAL', 'ALERT', 'EMERGENCY']:
        labels = e.labels or {}
        payload_text = e.payload if hasattr(e, "payload") else str(e)
        error_message = None
        if "Traceback" in payload_text:
            lines = payload_text.strip().split('\n')
            error_message = lines[-1]
            #print(f'error_msg:{error_message}')
        else:
            error_message = payload_text
        if (error_message):    
            error_messages.add(error_message)
        logs_entries.append({
            "timestamp": e.timestamp.isoformat() if e.timestamp else None,
            "payload": payload_text,
            "error_message": error_message,
            "process": labels.get("process"),
            "try_number": labels.get("try-number") or labels.get("try_number"),
            "task_id": labels.get("task-id") or labels.get("task_id"),
            "worker_id": labels.get("worker-id") or labels.get("worker_id"),
        })
      else:  
          processed_one, messages_one = err_sentence.process_log_entry(e)
          if processed_one:
              logs_entries_keywords.extend(processed_one)
          if messages_one:
              messages_keywords.update(messages_one)


    #logs_entries_keywords, messages_keywords = err_sentence.extract_error_messages(
    #        utils.build_log_filter_keywords(dag_id, run_id, test_id, test_start_date, end_with_padding), 500)

    return logs_entries, logs_entries_keywords, list(error_messages), list(messages_keywords)


def get_workload_id_from_logs(dag_id, run_id, test_id, test_start_date, end_with_padding, cluster_name, cluster_project):
    workload_id = None
    filter_str_workload_id = utils.build_log_filter_workload_id(
        dag_id, run_id, test_id, 
        test_start_date, end_with_padding
    )
    print(f"query log explorer workload_id")
    pattern = re.compile(r'Returned value was: (\S+)')
    entries_workload = utils.query_logs(filter_str_workload_id, LOGS_PROJECT_ID, 1)
    for e in entries_workload:
        payload_str = e.payload if hasattr(e, "payload") else str(e)
        match = pattern.search(payload_str)
        if match:
            workload_id = match.group(1)
            break

    return workload_id

def get_k8s_logs(filter_str_k8s, dag_id, run_id, test_id, cluster_name, cluster_project, workload_id):
    """Query and format K8s logs."""
    print(f'qeury k8s logs for workload_id:{workload_id},cluster_name:{cluster_name},project:{cluster_project}')
    #print(f"query log explorer {filter_str_k8s}")
    entries_k8s = utils.query_logs(filter_str_k8s, cluster_project, 50)
    # K8s log processing logic as per the original code (mostly print statements)

    messages = set()
    for entry in entries_k8s:
        #print("=== Log Entry ===")
        #print(f"Log name: {entry.log_name}")
        #print(f"Timestamp: {entry.timestamp}")
        #print(f"Severity: {entry.severity}")
        #print(type(entry))
        #if hasattr(entry, "payload"):
        #    print("Payload type:", type(entry.payload))
        #    print("Payload content:", entry.payload)
        #if hasattr(entry, "resource"):
        #    print("Resource:", entry.resource)
        #if hasattr(entry, "labels"):
        #    print("Labels:", entry.labels)

        payload = entry.payload

        # Case 1: already a dict
        if isinstance(payload, dict):
            data = payload
        # Case 2: string that looks like JSON
        elif isinstance(payload, str):
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                data = {"message": payload}
        else:
            data = {"message": str(payload)}

        # Now safely access nested fields
        message = data.get("message")  # top-level "message"
        #print("Message:", message)

        if (message):
            messages.add(message)
        #print(f'k8s messages:{messages}')    

        # comment out, Nested message are all None, not sure if further need.
        # If "message" lives inside a nested object
        #k8s_msg = (
        #    data.get("jsonPayload", {})
        #        .get("message")
        #    if "jsonPayload" in data else None
        #)
        #print("Nested message:", k8s_msg)

        # Debug: show keys available
        #print("Available keys in payload:", list(data.keys()))     
    return list(messages)


def process_test(row, dag_id, run_id, execution_date, test):
    """Process a single test, fetch logs, and build the output dictionary."""
    test_id, test_start_date, test_end_date = utils.get_test_run_dates(dag_id, test)
    test_status = test["test_status"]
    cluster_project = test["cluster_project"]
    cluster_name = test["cluster_name"]
    accelerator_type = test["accelerator_type"]
    accelerator_family = test["accelerator_family"]
    machine_families = test["machine_families"]

    if not test_start_date or not test_end_date:
        print(f"Skipping {dag_id} / {test_id} due to missing start/end date.")
        return None

    end_with_padding = test_end_date + timedelta(seconds=LOG_QUERY_END_PADDING_SECONDS)

    logs_entries = []
    logs_keywords_entries = []
    airflow_errors = []
    airflow_keywords = []
    k8s_messages = []
    workload_id = None
    log_url_k8s = ""

    if (not DAGS_TO_QUERY_LOGS or dag_id in DAGS_TO_QUERY_LOGS) and (test_status != 'success'):
        print(f"Processing {dag_id} / {test_id} test_duration:{test_start_date}-{test_end_date}")
        logs_entries, logs_keywords_entries, airflow_errors, airflow_keywords = get_airflow_logs(
                dag_id, run_id, test_id, test_start_date, end_with_padding, LOG_QUERY_SEVERITY)
        
        if cluster_name and cluster_project:
            workload_id = get_workload_id_from_logs(dag_id, run_id, test_id, test_start_date, end_with_padding, cluster_name, cluster_project)
            if (workload_id):
                filter_str_k8s = utils.build_log_filter_k8s(
                    dag_id, run_id, test_id, 
                    test_start_date, end_with_padding, cluster_name, workload_id)
                #k8s_messages = get_k8s_logs(filter_str_k8s, dag_id, run_id, test_id, cluster_name, cluster_project, workload_id)
                #if (k8s_messages):
                log_url_k8s = utils.build_log_explorer_url(filter_str_k8s, cluster_project)
        print(f'dag:{dag_id},test_id:{test_id},errors:{airflow_errors}')            
        print(f'dag:{dag_id},test_id:{test_id},keywords:{airflow_keywords}')            
        print(f'dag:{dag_id},test_id:{test_id},k8s_messages:{k8s_messages}')            


    filter_str_error = utils.build_log_filter(
        dag_id, run_id, test_id, 
        test_start_date, end_with_padding,
        severity=LOG_QUERY_SEVERITY
    )
    filter_str_all = utils.build_log_filter(
        dag_id, run_id, test_id, 
        test_start_date, end_with_padding
    )
    log_url_error = utils.build_log_explorer_url(filter_str_error, LOGS_PROJECT_ID) if test_status != 'success' else ""
    log_url_all = utils.build_log_explorer_url(filter_str_all, LOGS_PROJECT_ID)
    log_url_graph = f"https://efdb2a1d6c2c435b8b7de77690c286a9-dot-us-central1.composer.googleusercontent.com/dags/{quote(dag_id)}/grid?num_runs=25&search={quote(dag_id)}&tab=graph&dag_run_id={quote(run_id)}&task_id={quote(test_id)}"
    tasks_with_url = []
    for task in test["tasks"]:
        task_id = task["task_id"]
        task_url_airflow = (
            f"https://efdb2a1d6c2c435b8b7de77690c286a9-dot-us-central1.composer.googleusercontent.com/dags/{quote(dag_id)}/grid?search={quote(dag_id)}&num_runs=25&dag_run_id={quote(run_id)}&task_id={quote(task_id)}&tab=logs"    
        )
        tasks_with_url.append({
            "task_id": task["task_id"],
            "task_start_date": task["task_start_date"],
            "task_end_date": task["task_end_date"],
            "try_number": task["try_number"],
            "state": task["state"],
            "task_url_airflow": task_url_airflow,
        })

    #print(f'url_graph:{log_url_graph}')
    #print(f'tasks:{tasks_with_url}')


    return {
        "test_id": test_id,
        "test_start_date": test_start_date,
        "test_end_date": test_end_date,
        "test_status": test["test_status"],
        "cluster_project": cluster_project,
        "cluster_name": cluster_name,
        "accelerator_type": accelerator_type,
        "accelerator_family": accelerator_family,
        "machine_families": machine_families,
        "workload_id": workload_id,
        "tasks": tasks_with_url,
        "logs": logs_entries,
        "logs_keywords": logs_keywords_entries,
        "airflow_errors": airflow_errors,
        "airflow_keywords": airflow_keywords,
        "k8s_messages": k8s_messages,
        "log_url_error": log_url_error,
        "log_url_all": log_url_all,
        "log_url_k8s": log_url_k8s,
        "log_url_graph": log_url_graph
    }


def save():
    start_date = datetime.now()
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)

    #query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME}` where dag_id='jax_ai_image_gpu_e2e'"
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
            bigquery.SchemaField("workload_id", "STRING"),
            bigquery.SchemaField("cluster_project", "STRING"),
            bigquery.SchemaField("cluster_name", "STRING"),
            bigquery.SchemaField("accelerator_type", "STRING"),
            bigquery.SchemaField("accelerator_family", "STRING"),
            bigquery.SchemaField("machine_families", "STRING", mode="REPEATED"),
            bigquery.SchemaField("tasks", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("task_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("task_start_date", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("task_end_date", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("try_number", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("task_url_airflow", "STRING"),
            ]),
            bigquery.SchemaField("logs", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("payload", "STRING"),
                bigquery.SchemaField("error_message", "STRING"),
                bigquery.SchemaField("process", "STRING"),
                bigquery.SchemaField("task_id", "STRING"),
                bigquery.SchemaField("try_number", "STRING"),
                bigquery.SchemaField("worker_id", "STRING"),
            ]),

            bigquery.SchemaField("logs_keywords", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("timestamp", "TIMESTAMP"),
                bigquery.SchemaField("severity", "STRING"),
                bigquery.SchemaField("keywords", "STRING", mode="REPEATED"),
                bigquery.SchemaField("sentence", "STRING"),
                bigquery.SchemaField("payload", "STRING"),
                bigquery.SchemaField("process", "STRING"),
                bigquery.SchemaField("task_id", "STRING"),
                bigquery.SchemaField("try_number", "STRING"),
                bigquery.SchemaField("worker_id", "STRING"),
            ]),
            bigquery.SchemaField("airflow_errors", "STRING", mode="REPEATED"),
            bigquery.SchemaField("airflow_keywords", "STRING", mode="REPEATED"),
            bigquery.SchemaField("k8s_messages", "STRING", mode="REPEATED"),
            bigquery.SchemaField("log_url_error", "STRING"),
            bigquery.SchemaField("log_url_all", "STRING"),
            bigquery.SchemaField("log_url_k8s", "STRING"),
            bigquery.SchemaField("log_url_graph", "STRING")
        ])
    ]

    for row in rows:
        dag_id, run_id, execution_date, run_start_date, run_end_date = utils.get_dag_run_info(row)
        
        tests_data = []
        for test in row["tests"]:
            test_data = process_test(row, dag_id, run_id, execution_date, test)
            if test_data:
                tests_data.append(test_data)

        output_data.append({
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": execution_date,
            "run_start_date": run_start_date,
            "run_end_date": run_end_date,
            "run_status": row.run_status,
            "tests": tests_data
        })

    utils.save_to_gcs_and_load_bq(output_data, schema, BQ_DEST_TABLE)
    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


if __name__ == "__main__":
    save()
