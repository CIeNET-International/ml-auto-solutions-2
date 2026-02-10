from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 
from airflow.utils.trigger_rule import TriggerRule
from dags.dashboard.statistic import persist_view, save_cluster_from_log, save_all_clusters, save_parse, save_cluster_status, save_log, save_cluster_monitoring

with DAG(
    dag_id="dashboard_statistic",
    start_date=datetime(2025, 1, 1),
    #schedule_interval=None,  
    #schedule="@hourly",
    #schedule="0 */4 * * *",
    schedule="25 0 * * *",
    catchup=False,
    tags=["dashboard"],
    default_args={"retries": 0},
) as dag:

    task_save_parse = PythonOperator(
        task_id="save_parse",
        python_callable=save_parse.save
    )

    task_persist_all_dag_base = PythonOperator(
        task_id="persist_all_dag_base",
        python_callable=persist_view.persist,
        op_args=["all_dag_base"]
    )

    task_persist_base = PythonOperator(
        task_id="persist_base",
        python_callable=persist_view.persist,
        op_args=["base"]
    )

    task_save_cluster_from_log = PythonOperator(
        task_id="save_cluster_from_log",
        python_callable=save_cluster_from_log.save
    )

    task_save_all_clusters = PythonOperator(
        task_id="save_all_clusters",
        python_callable=save_all_clusters.save
    )

    task_persist_run_status = PythonOperator(
        task_id="persist_run_status",
        python_callable=persist_view.persist,
        op_args=["run_status"]
    )

    task_save_cluster_status = PythonOperator(
        task_id="save_cluster_status",
        python_callable=save_cluster_status.save
    )

    task_persist_statistic_data = PythonOperator(
        task_id="persist_statistic_data",
        python_callable=persist_view.persist,
        op_args=["statistic_data"]
    )

    task_persist_statistic = PythonOperator(
        task_id="persist_statistic",
        python_callable=persist_view.persist,
        op_args=["statistic"]
    )

    task_persist_dag_cluster_map = PythonOperator(
        task_id="persist_dag_cluster_map",
        python_callable=persist_view.persist,
        op_args=["dag_cluster_map"]
    )

    task_persist_test = PythonOperator(
        task_id="persist_test",
        python_callable=persist_view.persist,
        op_args=["test"]
    )

    task_persist_dag = PythonOperator(
        task_id="persist_dag",
        python_callable=persist_view.persist,
        op_args=["dag"]
    )

    task_persist_profile = PythonOperator(
        task_id="persist_profile",
        python_callable=persist_view.persist,
        op_args=["profile"]
    )

    task_persist_sch = PythonOperator(
        task_id="persist_sch",
        python_callable=persist_view.persist,
        op_args=["sch"]
    )

    task_save_log = PythonOperator(
        task_id="save_log",
        python_callable=save_log.save,
        retries=3,                  
        retry_delay=timedelta(minutes=30)
    )

    task_save_cluster_monitoring = PythonOperator(
        task_id="save_cluster_monitoring",
        python_callable=save_cluster_monitoring.save,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=3,                  
        retry_delay=timedelta(minutes=30)
    )

    task_persist_dag_duration = PythonOperator(
        task_id="persist_dag_duration",
        python_callable=persist_view.persist,
        op_args=["dag_duration"]
    )

    task_persist_test_duration = PythonOperator(
        task_id="persist_test_duration",
        python_callable=persist_view.persist,
        op_args=["test_duration"]
    )

    task_persist_gke_cluster_info = PythonOperator(
        task_id="persist_gke_cluster_info",
        python_callable=persist_view.persist,
        op_args=["gke"]
    )

    task_persist_global = PythonOperator(
        task_id="persist_global",
        python_callable=persist_view.persist,
        op_args=["global"]
    )

    # order dependencies
    task_save_parse >>  task_persist_all_dag_base >> task_persist_base >> task_save_cluster_from_log >> task_save_all_clusters
    [task_persist_base] >> task_persist_run_status
    [task_persist_base] >> task_persist_dag_duration
    [task_persist_base] >> task_persist_test_duration
    [task_save_all_clusters] >> task_save_cluster_status >> task_persist_statistic_data >> task_persist_statistic >> task_persist_dag >> task_save_log >> task_save_cluster_monitoring
    [task_save_all_clusters] >> task_persist_profile >> task_persist_sch
    [task_persist_statistic, task_save_cluster_from_log] >> task_persist_global
    [task_save_cluster_status, task_persist_statistic] >> task_persist_dag_cluster_map
    [task_save_cluster_status] >> task_persist_gke_cluster_info
    [task_persist_statistic, task_persist_dag_cluster_map] >> task_persist_dag
    [task_persist_statistic, task_persist_dag_cluster_map] >> task_persist_test


