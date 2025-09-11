CREATE OR REPLACE VIEW `amy_xlml_poc_prod.last_one_run_wo_cluster` AS

WITH dag_with_clusters AS (
  SELECT distinct dag_id
  FROM `amy_xlml_poc_prod.cluster_info_view_latest_db`  
), 
    
last_run_tests AS (
  SELECT base.dag_id, base.tags, runs.run_id, runs.execution_date, runs.start_date run_start_date, runs.end_date run_end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.test_id, tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE base.dag_id NOT IN (SELECT dag_id FROM dag_with_clusters) 
    AND run_order_desc=1
)


SELECT
  dag_id,
  tags,
  run_id,
  execution_date,
  run_start_date,
  run_end_date,
  test_id,
  test_start_date,
  test_end_date
FROM last_run_tests

