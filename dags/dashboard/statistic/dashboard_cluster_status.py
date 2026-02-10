from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from airflow.utils.trigger_rule import TriggerRule
from dags.dashboard.statistic import persist_view, save_cluster_from_log, save_all_clusters, save_parse, save_cluster_status, save_log, save_cluster_monitoring

with DAG(
    dag_id="dashboard_cluster_status",
    start_date=datetime(2025, 1, 1),
    #schedule_interval=None,  
    schedule="@hourly",
    #schedule="0 */4 * * *",
    #schedule="25 0 * * *",
    catchup=False,
    tags=["dashboard"],
    default_args={"retries": 0},
) as dag:
    task_save_cluster_status = PythonOperator(
        task_id="save_cluster_status",
        python_callable=save_cluster_status.save
    )

    task_persist_global = PythonOperator(
        task_id="persist_global",
        python_callable=persist_view.persist,
        op_args=["global"]
    )

    # order dependencies
    task_save_cluster_status >> task_persist_global


