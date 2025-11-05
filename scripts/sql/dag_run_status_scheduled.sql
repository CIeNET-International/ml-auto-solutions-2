CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_scheduled` AS


SELECT
  dag_id,
  dag_owner,
  tags,
  category,
  accelerator,
  is_paused,
  formatted_schedule,
  total_tests_all,
  is_quarantined,    
  date,
  daily_status_scheduled.status AS status,
  daily_status_scheduled.total_tests AS total_tests,
  daily_status_scheduled.successful_tests AS successful_tests,
  daily_status_scheduled.failed_tests AS failed_tests
FROM
  `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_base`
WHERE is_quarantined_dag = FALSE
  
