CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_test_duration_stat_view` AS

SELECT
  t.dag_id, t.test_id,
  t.dag_owners, t. tags,  t.category, t.accelerator, t.formatted_schedule,
  --a.cluster_name, a.project_name, a.region,
  success_run_count,
  any_run_count,
  success_test_count,
  avg_duration_success_seconds,
  max_duration_success_seconds,
  avg_duration_test_success_seconds,
  max_duration_test_success_seconds,  
  avg_duration_test_failed_seconds,
  max_duration_test_failed_seconds,  
  avg_duration_any_seconds,
  max_duration_any_seconds,
  last_success_execution_date_tasks,
  last_success_start_date_tasks,
  last_success_end_date_tasks,
  last_success_duration_seconds_tasks,
  last_success_execution_date_dagrun,
  last_success_start_date_dagrun,
  last_success_end_date_dagrun,
  last_success_duration_seconds_dagrun,
  is_quarantined_dag,
  is_quarantined_test,
  last_test_success_run_start_date,
  last_test_succuss_test_duration_seconds
FROM `cienet-cmcs.amy_xlml_poc_prod.dag_test_duration_stat_iq` t
--LEFT JOIN `cienet-cmcs.amy_xlml_poc_prod.all_test` a ON t.dag_id = a.dag_id AND t.test_id = a.test_id
WHERE is_quarantined_test = FALSE

