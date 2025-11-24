CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_quarantined` AS

WITH
quarantined_dag AS (
  SELECT DISTINCT dag_id FROM `cienet-cmcs.amy_xlml_poc_prod.base` where ARRAY_LENGTH(test_ids_q)>0
)

SELECT
  b.dag_id,
  dag_owner,
  tags,
  category,
  accelerator,
  is_paused,
  formatted_schedule,
  total_tests_all,
  is_quarantined,        
  date,
  daily_status_quarantined.status AS status,
  daily_status_quarantined.total_tests AS total_tests,
  daily_status_quarantined.successful_tests AS successful_tests,
  daily_status_quarantined.failed_tests AS failed_tests,
  daily_status_quarantined.total_run_tests AS total_run_tests    
FROM
  `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_base` b
JOIN quarantined_dag q ON b.dag_id = q.dag_id
  
