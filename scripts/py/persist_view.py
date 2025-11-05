import sys
from google.cloud import bigquery
from datetime import datetime
from config import BQ_PROJECT_ID, BQ_DATASET, PERSIST_PAIR

def persist_view_to_table(pair_name: str):
    """
    Materialize a specific BigQuery view into a table for a given environment.
    """
    # Direct dictionary lookup for the pair
    target_pair = PERSIST_PAIR.get(pair_name)
    if not target_pair:
        raise ValueError(f"Unknown pair '{pair_name}'. "
                         f"Available pairs: {list(PERSIST_PAIR.keys())}")

    project_id = BQ_PROJECT_ID
    dataset_id = BQ_DATASET
    view_name = target_pair["view_name"]
    table_name = target_pair["table_name"]

    client = bigquery.Client(project=project_id)

    view_id = f"{project_id}.{dataset_id}.{view_name}"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    sql = f"SELECT * FROM `{view_id}`"

    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition="WRITE_TRUNCATE"
    )

    query_job = client.query(sql, job_config=job_config)
    query_job.result()

    print(f"View `{view_id}` for pair '{pair_name}' "
          f"persisted as table `{table_id}`")

def persist(pair_name:str):
    start_time = datetime.now()
    persist_view_to_table(pair_name)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f'start: {start_time}, end: {end_time}, duration: {duration}')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <pair_name>")
        print(f"Available pairs: {list(PERSIST_PAIR.keys())}")
        sys.exit(1)

    persist(sys.argv[1])

