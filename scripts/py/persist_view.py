import sys
from google.cloud import bigquery
from config_persist import ENV, PERSIST_PAIR
from datetime import datetime

def persist_view_to_table(env: str, pair_name: str):
    """
    Materialize a specific BigQuery view into a table for a given environment.
    """
    if env not in ENV:
        raise ValueError(f"Unknown environment '{env}'. "
                         f"Available options: {list(ENV.keys())}")
    
    # Direct dictionary lookup for the pair
    target_pair = PERSIST_PAIR.get(pair_name)
    if not target_pair:
        raise ValueError(f"Unknown pair '{pair_name}'. "
                         f"Available pairs: {list(PERSIST_PAIR.keys())}")

    settings = ENV[env]
    project_id = settings["project_id"]
    dataset_id = settings["dataset_id"]
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

    print(f"{env.upper()}: View `{view_id}` for pair '{pair_name}' "
          f"persisted as table `{table_id}`")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <env> <pair_name>")
        print(f"Available envs: {list(ENV.keys())}")
        print(f"Available pairs: {list(PERSIST_PAIR.keys())}")
        sys.exit(1)

    start_time = datetime.now()
    env = sys.argv[1]
    pair_name = sys.argv[2]
    persist_view_to_table(env, pair_name)

    end_time = datetime.now()
    duration = end_time - start_time
    print(f'start: {start_time}, end: {end_time}, duration: {duration}')

