CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status` AS


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
  daily_status.status AS status,
  daily_status.total_tests AS total_tests,
  daily_status.successful_tests AS successful_tests,
  daily_status.failed_tests AS failed_tests
FROM
  `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_base`
WHERE is_quarantined_dag = FALSE

  
