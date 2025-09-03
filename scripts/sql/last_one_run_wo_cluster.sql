CREATE OR REPLACE VIEW `amy_xlml_poc_prod.last_one_run_wo_cluster` AS

WITH dag_with_clusters AS (
  SELECT distinct dag_id
  FROM `amy_xlml_poc_prod.cluster_info_view_latest_db`  
), 
    
latest_runs AS (
  SELECT
    dr.dag_id,
    dr.run_id,
    dr.execution_date,
    dr.start_date AS run_start_date,
    dr.end_date   AS run_end_date
  FROM 
    `amy_xlml_poc_prod.dag_run` dr
  WHERE 
    dr.dag_id not in (select dag_id from dag_with_clusters) AND
    dr.start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND dr.end_date IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dr.dag_id ORDER BY dr.execution_date DESC) = 1
),

-- Check last trial only
last_task_status AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.task_id,
    ti.start_date,
    ti.end_date,
    ti.state,
    ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn
  FROM
    `amy_xlml_poc_prod.task_instance` AS ti
),    
    
-- 2) Filter task_instance early + extract test_id
ti_scoped AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.start_date,
    ti.end_date,
    ti.state
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN latest_runs lr
    ON ti.dag_id = lr.dag_id
   AND ti.run_id = lr.run_id
),

-- 3) Aggregate per test_id (no nested aggregates)
test_summaries AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    MIN(start_date) AS test_start_date,
    MAX(end_date)   AS test_end_date,
    COUNTIF(state != 'success') AS failed_in_test
  FROM ti_scoped
  GROUP BY dag_id, run_id, test_id
)

-- 4) Final output
SELECT
  lr.dag_id,
  lr.run_id,
  lr.execution_date,
  lr.run_start_date,
  lr.run_end_date,
  ts.test_id,
  ts.test_start_date,
  ts.test_end_date
FROM latest_runs lr
JOIN test_summaries ts
  USING (dag_id, run_id)



