CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_manual` AS

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
  daily_status_manual.status AS status,
  daily_status_manual.total_tests AS total_tests,
  daily_status_manual.successful_tests AS successful_tests,
  daily_status_manual.failed_tests AS failed_tests
FROM
  `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_base`
WHERE is_quarantined_dag = FALSE

