CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_duration_stat` AS

SELECT
  dag_id,
  owners,
  tags, 
  category, 
  accelerator,    
  formatted_schedule, 
  total_tests, total_tests_q, total_tests_qe,  
  success_run_count,
  any_run_count,
  avg_duration_success_seconds,
  max_duration_success_seconds,
  avg_duration_any_seconds,
  max_duration_any_seconds,
  last_success_execution_date_tasks,
  last_success_start_date_tasks,
  last_success_end_date_tasks,
  last_success_duration_seconds_tasks,
  last_success_run_id,   
  last_success_execution_date_dagrun,
  last_success_start_date_dagrun,
  last_success_end_date_dagrun,
  last_success_duration_seconds_dagrun
FROM `amy_xlml_poc_prod.dag_duration_stat_iq`  
WHERE is_quarantined_dag  = FALSE

