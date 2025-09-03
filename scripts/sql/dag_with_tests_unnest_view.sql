CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_with_tests_unnest_view` AS
SELECT
  -- DAG-level fields
  ds.dag_id,
  ds.owners,
  ds.tags,  
  ds.success_run_count AS dag_success_run_count,
  ds.any_run_count AS dag_any_run_count,
  ds.avg_duration_success_seconds AS dag_avg_duration_success_seconds,
  ds.max_duration_success_seconds AS dag_max_duration_success_seconds,
  ds.avg_duration_any_seconds AS dag_avg_duration_any_seconds,
  ds.max_duration_any_seconds AS dag_max_duration_any_seconds,
  ds.last_success_execution_date_tasks AS dag_last_success_execution_date_tasks,
  ds.last_success_start_date_tasks AS dag_last_success_start_date_tasks,
  ds.last_success_end_date_tasks AS dag_last_success_end_date_tasks,
  ds.last_success_duration_seconds_tasks AS dag_last_success_duration_seconds_tasks,
  ds.last_success_run_id AS dag_last_success_run_id,
  ds.last_success_execution_date_dagrun AS dag_last_success_execution_date_dagrun,
  ds.last_success_start_date_dagrun AS dag_last_success_start_date_dagrun,
  ds.last_success_end_date_dagrun AS dag_last_success_end_date_dagrun,
  ds.last_success_duration_seconds_dagrun AS dag_last_success_duration_seconds_dagrun,

  -- Test-level fields
  ts.test_id,
  ts.success_run_count AS test_success_run_count,
  ts.any_run_count AS test_any_run_count,
  ts.avg_duration_success_seconds AS test_avg_duration_success_seconds,
  ts.max_duration_success_seconds AS test_max_duration_success_seconds,
  ts.avg_duration_any_seconds AS test_avg_duration_any_seconds,
  ts.max_duration_any_seconds AS test_max_duration_any_seconds,
  ts.last_success_execution_date_tasks AS test_last_success_execution_date_tasks,
  ts.last_success_start_date_tasks AS test_last_success_start_date_tasks,
  ts.last_success_end_date_tasks AS test_last_success_end_date_tasks,
  ts.last_success_duration_seconds_tasks AS test_last_success_duration_seconds_tasks,
  ts.last_success_execution_date_dagrun AS test_last_success_execution_date_dagrun,
  ts.last_success_start_date_dagrun AS test_last_success_start_date_dagrun,
  ts.last_success_end_date_dagrun AS test_last_success_end_date_dagrun,
  ts.last_success_duration_seconds_dagrun AS test_last_success_duration_seconds_dagrun
FROM `amy_xlml_poc_prod.dag_duration_stat` ds
LEFT JOIN `amy_xlml_poc_prod.dag_test_duration_stat` ts
  ON ds.dag_id = ts.dag_id;

