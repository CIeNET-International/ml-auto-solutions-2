import re
import json
import urllib.parse
import time
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery, storage, logging_v2
import save_log_utils as utils
from config import (
    BQ_PROJECT_ID, BQ_DATASET, 
    GCS_PROJECT_ID, GCS_BUCKET_NAME,
    LOG_PROJECT_ID
)

BQ_VIEW_NAME_NO_CLUSTER = "last_one_run_wo_cluster"
BQ_DEST_TABLE_CLUSTER_FROM_LOG = "cluster_info_from_log"

def parse_zulu(dt_str):
    """Parse Zulu-format datetime like 2025-08-10T00:00:00Z"""
    return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def json_serial(obj):
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


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

def build_log_filter(dag_id, run_id, task_id_prefix, exec_date, start_time, end_time, search_keyword):
    extracted_duration = extract_date_string_from_run_id(run_id)
    """Build log explorer filter string."""
    filter_parts = [
        f'resource.type="cloud_composer_environment"',
        f'resource.labels.environment_name="ml-automation-solutions"',
        f'labels.workflow="{dag_id}"',
        f'labels.execution-date="{extracted_duration}"',
        f'labels.task-id=~"^{task_id_prefix}.*"',
        f'timestamp >= "{start_time.isoformat()}"',
        f'timestamp <= "{end_time.isoformat()}"',
        #f'SEARCH("{search_keyword}")'
    ]
    search_condition = f'(textPayload: "response.json" OR textPayload: "xlml_metadata" OR SEARCH("{search_keyword}"))'
    filter_parts.append(search_condition)

    return "\n".join(filter_parts)

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
    table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_DEST_TABLE_CLUSTER_FROM_LOG}"

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()
    print(f"Loaded {len(data)} rows to {table_id}.")

def test():
    filter_parts = [
        f'resource.type="cloud_composer_environment"',
        f'resource.labels.environment_name="camilo-orbax-dev"',
        f'timestamp>="2025-09-18T21:08:46.608Z"'
    ]
    search_condition = f'(textPayload: "xlml_metadata")'
    filter_parts.append(search_condition)

    filter_str = "\n".join(filter_parts)
    print(f'{filter_str}')

    entries = utils.query_logs(filter_str, LOG_PROJECT_ID, 10)
    for e in entries:
        payload_str = e.payload if hasattr(e, "payload") else str(e)

        if 'xlml_metadata' in payload_str:
            parsed_result = parse_cluster_metadata(payload_str)
            if parsed_result:
                import pprint
                pprint.pprint(parsed_result)    
                break



def parse_cluster_metadata(payload_string):
    # 1. Check for and remove the 'xlml_metadata ' prefix.
    #    The `strip()` method is used to remove any leading/trailing whitespace.
    cleaned_string = payload_string.lstrip().replace('xlml_metadata ', '', 1).strip()
    print(f'xlml_metadata...{cleaned_string}')
    try:
        # 2. Parse the cleaned string as a JSON object.
        metadata = json.loads(cleaned_string)
        accelerator = metadata.get("accelerator")
        if accelerator:
            print(f'***accelerator found {accelerator.get("name")}')
        if metadata and metadata.get("cluster_project"):
            print('*********************************************************')
            print('*********************************************************')
            return metadata.get("cluster_project"), metadata.get("zone"), metadata.get("cluster_project")

        # 3. Extract and organize the relevant fields.
        parsed_data = {
            "cluster_info": {
                "project": metadata.get("cluster_project"),
                "zone": metadata.get("zone"),
                "name": metadata.get("cluster_name"),
                "accelerator_type": metadata.get("accelerator_type"),
                "num_slices": metadata.get("num_slices")
            },
            "workload_info": {
                "workload_id": metadata.get("workload_id"),
                "benchmark_id": metadata.get("benchmark_id"),
                "task_id": metadata.get("task_id"),
                "docker_image": metadata.get("docker_image"),
                "gcs_path": metadata.get("gcs_path")
            }
        }
        return None, None, None

        #return parsed_data
    except json.JSONDecodeError as e:
        print(f"Error: Failed to decode JSON payload after cleaning. Details: {e}")
        return None, None, None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None, None, None
    if parsed_data:
        return parsed_date.name, parsed_data.zone, parsed_data.project
    return None, None, None


def get_cluster_info_from_logs(dag_id, run_id, test_id, test_start_date, test_end_date):
    cluster_name = None
    cluster_region = None
    project_name = None
    filter_str = build_log_filter(
        dag_id, run_id, test_id, extract_execution_date_from_run_id(run_id),
        test_start_date, test_end_date, "gcloud container clusters get-credentials"
    )

    payload_str = None
    from_cred = False
    #pattern = re.compile(r"gcloud container clusters get-credentials\s+(\S+)")
    #pattern = re.compile(
    #    r'gcloud container clusters get-credentials\s+([^\s]+)'  
    #    r'(?:\s+(?:--region|--zone)\s+([^\s]+))?',             
    #    re.IGNORECASE
    #)

    #pattern_gke_credentials = re.compile(
    #    r"gcloud\s+container\s+clusters\s+get-credentials\s+([^\s]+)"  # Group 1: Cluster Name
    #    r"(?:\s+(?:--region|--zone)\s+([^\s]+))?"                    # Group 2 (Optional): Location (Region or Zone)
    #    r"(?:.*?\s+--project\s+([a-z0-9-]+))",                         # Group 3: Project ID
    #    re.IGNORECASE | re.DOTALL  # DOTALL allows '.' to match newlines if logs are multiline
    #)
    pattern_gke_credentials = re.compile(
        r"gcloud\s+container\s+clusters\s+get-credentials\s+([^\s;]+)" # Group 1: Cluster Name (Your core working part)
        r"(?:"                                                        # Start non-capturing group for location
        r".*?"                                                    # Non-greedy match for anything between name and location flag
        r"[\s;]+"                                                 # Separator (space or semicolon)
        r"(?:--region|--zone)"                                    # Match --region OR --zone
        r"[\s;]+"                                                 # Separator
        r"([^\s;]+)"                                              # Group 2: Captures the Location
        r")?"                                                         # Make Location Optional
        r"(?:"                                                        # Start non-capturing group for project
        r".*?"                                                    # Non-greedy match for anything between name/location and project flag
        r"[\s;]+"                                                 # Separator
        r"--project"                                              # Match --project
        r"[\s;]+"                                                 # Separator
        r"([a-z0-9-]+)"                                           # Group 3: Captures the Project ID
        r")?",                                                        # Make Project ID Optional
        re.IGNORECASE | re.DOTALL
    )
    # Region/Zone value
    pattern_region = re.compile(
        r"(?:--region|--zone)[=\s]([^\s;]+)",
        re.IGNORECASE
    )
    # Project ID value
    pattern_project = re.compile(
        r"--project[=\s]([^\s;]+)",
        re.IGNORECASE
    )
    entries = utils.query_logs(filter_str, LOG_PROJECT_ID, 10)
    for e in entries:
        payload_str = e.payload if hasattr(e, "payload") else str(e)
        match_gke = pattern_gke_credentials.search(payload_str)
        if match_gke:
            print(f'log: {payload_str}')
            cluster_name = match_gke.group(1)
            cluster_region = match_gke.group(2) # Will be None if --region/--zone not used
            project_name = match_gke.group(3)
            if (cluster_name and cluster_region is None):
                region_match = pattern_region.search(payload_str)
                cluster_region = region_match.group(1) if region_match else None
            if (cluster_name and project_name is None):
                project_match = pattern_project.search(payload_str)
                project_name = project_match.group(1) if project_match else None
            print(f'region:{cluster_region} project:{project_name}')
            from_cred = True
            break 
        elif 'response.json' in payload_str:
            cluster_name, cluster_region, project_name = parse_gke_info_from_json(payload_str)
            if (cluster_name):
                break;
        elif 'xlml_metadata' in payload_str:
            #metadata = parse_cluster_metadata(payload_str)
            cluster_name, cluster_region, project_name = parse_cluster_metadata(payload_str)
            if (cluster_name):
                break


    if (cluster_name and from_cred and project_name is None):
        pattern_project_1 = re.compile(r"gcloud\s+config\s+set\s+project\s+([a-z0-9-]+)")
        #pattern_project_2 = re.compile(
        #    r"gcloud\s+container\s+clusters\s+get-credentials\s+[^\s]+"
        #    r"(?:\s+--region\s+[^\s]+)?"
        #    r"\s+--project\s+([a-z0-9-]+)"
        #)
        #match = pattern_project.search(payload_str)
        #if match:
        #    project_name = match.group(1)
        #for regex in (pattern_project_1, pattern_project_2):
        match = pattern_project_1.search(payload_str)
        if match:
            project_name = match.group(1)
        #        break
    return cluster_name, cluster_region, project_name

def parse_gke_info_from_json(log_string):
    """
    Parses a log string to extract the GKE cluster name, region, and project.

    Args:
        log_string: The log entry as a string.

    Returns:
        A tuple containing (gke_cluster_name, region_id, project_id) or (None, None, None) if not found.
    """
    print(f'in gke_json parse......')
    try:
        # Use regex to extract the JSON-like part of the string.
        # This regex will now specifically look for the content after 'response.json() '
        # which starts the dictionary.
        match = re.search(r"response\.json\(\)\s*(\{.*\})", log_string, re.DOTALL)
        if not match:
            return None, None, None

        # The captured group is the JSON-like string.
        json_string = match.group(1).replace("'", '"')

        # Find the last closing brace and slice the string up to that point
        # to ensure valid JSON structure.
        last_brace_index = json_string.rfind('}')
        if last_brace_index != -1:
            json_string = json_string[:last_brace_index + 1]

        # Safely load the JSON string into a Python dictionary.
        data = json.loads(json_string)

        # Access the nested keys to get the GKE cluster path.
        gke_path = data.get('config', {}).get('gkeCluster')

        if gke_path:
            # The GKE path format is: projects/PROJECT_ID/locations/REGION/clusters/CLUSTER_NAME
            parts = gke_path.split('/')

            # Check if the path has the expected number of parts (at least 6: p/ID/l/LOC/c/NAME)
            if len(parts) >= 6 and parts[0] == 'projects' and parts[2] == 'locations' and parts[4] == 'clusters':
                # parts[1] is the project ID
                project_id = parts[1]

                # parts[3] is the region/location
                region_id = parts[3]

                # parts[5] is the cluster name (or parts[-1] if the path is longer)
                cluster_name = parts[-1]

                # IMPORTANT: Update the return order as requested
                return cluster_name, region_id, project_id

            # Fallback for old/unexpected path formats (only getting project and cluster name)
            elif len(parts) >= 2:
                project_id = parts[1]
                cluster_name = parts[-1]
                return cluster_name, None, project_id


    except (json.JSONDecodeError, AttributeError, IndexError, TypeError) as e:
        print(f"Error parsing log: {e}")
        return None, None, None

    return None, None, None

def parse_gke_info_from_json_old(log_string):
    """
    Parses a log string to extract the GKE cluster name and its project.

    Args:
        log_string: The log entry as a string.

    Returns:
        A tuple containing (project_id, gke_cluster_name) or (None, None) if not found.
    """
    print(f'in gke_json parse......')
    try:
        # Use regex to extract the JSON-like part of the string.
        # This part assumes the log message is a Python dictionary representation.
        match = re.search(r"(\{.*\})", log_string)
        if not match:
            return None, None, None

        # Convert the string representation of a dictionary to a valid JSON string.
        # This handles the use of single quotes and the trailing comma issue.
        json_string = match.group(1).replace("'", '"')

        # Handle the case where the log string might have a trailing part that breaks JSON.
        # For example, "..." or a closing brace that's not part of the dictionary.
        # This regex will only capture up to the point of the valid JSON structure.
        # Find the last closing brace and slice the string up to that point.
        last_brace_index = json_string.rfind('}')
        if last_brace_index != -1:
            json_string = json_string[:last_brace_index+1]

        # Safely load the JSON string into a Python dictionary.
        data = json.loads(json_string)

        # Access the nested keys to get the GKE cluster path.
        gke_path = data.get('config', {}).get('gkeCluster')

        if gke_path:
            # Split the path to get the project and cluster name.
            parts = gke_path.split('/')
            project_id = parts[1]
            cluster_name = parts[-1]
            return cluster_name, None, project_id

    except (json.JSONDecodeError, AttributeError, IndexError) as e:
        print(f"Error parsing log: {e}")
        return None, None, None

def process_test(dag_id, run_id, execution_date, test_id, test_start_date, test_end_date):
    """Process a single test, fetch logs, and build the output dictionary."""
    project_name = None
    cluster_name = None
    region = None

    if not test_start_date or not test_end_date:
        print(f"Skipping {dag_id} / {test_id} due to missing start/end date.")
        return cluster_name, region, project_name

    cluster_name, region, project_name = get_cluster_info_from_logs(dag_id, run_id, test_id, test_start_date, test_end_date)
    #print(f'dag:{dag_id},test_id:{test_id},cluster_name:{cluster_name},project:{project_name}')            

    return cluster_name, region, project_name


def save():
    start_date = datetime.now()
    bq_client = bigquery.Client(project=BQ_PROJECT_ID)

    #query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}` where dag_id='a3mega_recipes_mixtral-8x7b_nemo'"
    #query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}` where dag_id='mlcompass_simple_dag'"
    #query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}` where dag_id='maxtext_regular_save'"
    #query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}` where dag_id='pw_mcjax_benchmark_recipe' and test_id='clean_up_pod'"
    query = f"SELECT * FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_VIEW_NAME_NO_CLUSTER}`"
    rows = list(bq_client.query(query).result())

    schema = [
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("test_id", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("run_start_date", "TIMESTAMP"),
        bigquery.SchemaField("cluster_name", "STRING"),
        bigquery.SchemaField("region", "STRING"),
        bigquery.SchemaField("project_name", "STRING"),
    ]

    output_data = []
    for row in rows:
        cluster_name, region, project_name = process_test(row.dag_id, row.run_id, row.execution_date, row.test_id, row.test_start_date, row.test_end_date)
        print(f'{row.dag_id}.{row.test_id} cluster:{cluster_name},region:{region},project:{project_name}')

        #if cluster_name and project_name:
        if cluster_name:
          output_data.append({
            "dag_id": row.dag_id,
            "test_id": row.test_id,
            "run_id": row.run_id,
            "run_start_date": row.run_start_date,
            "cluster_name": cluster_name,
            "region": region,
            "project_name":project_name 
          })

    save_to_gcs_and_load_bq(output_data, schema)
    end_date = datetime.now()
    dur = end_date - start_date
    print(f'start:{start_date}, end_date:{end_date}, duration:{dur} seconds')


if __name__ == "__main__":
    save()
