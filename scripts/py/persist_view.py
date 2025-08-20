import sys
from google.cloud import bigquery
from config_env import CONFIG

def persist_view_to_table(env: str):
    """
    Materialize a BigQuery view into a table for a given environment.
    """
    if env not in CONFIG:
        raise ValueError(f"Unknown environment '{env}'. "
                         f"Available options: {list(CONFIG.keys())}")

    settings = CONFIG[env]

    project_id = settings["project_id"]
    dataset_id = settings["dataset_id"]
    view_name = settings["view_name"]
    table_name = settings["table_name"]

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

    print(f"{env.upper()}: View `{view_id}` persisted as table `{table_id}`")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <env>")
        print(f"Available envs: {list(CONFIG.keys())}")
        sys.exit(1)

    env = sys.argv[1]
    persist_view_to_table(env)

