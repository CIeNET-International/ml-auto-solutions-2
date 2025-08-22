
CREATE OR REPLACE VIEW `amy_xlml_poc_2.dag_run_statistic` AS
-- DAGs with the specified tag
WITH dag_with_tag AS (
  SELECT dt.dag_id,ARRAY_AGG(name) as tags
  FROM `amy_xlml_poc_2.dag_tag` dt
  GROUP BY dag_id  
),

-- DAG runs that dag is is_active
dag_runs AS (
  SELECT dr.dag_id, dr.run_id
  FROM `amy_xlml_poc_2.dag_run` dr
  JOIN `amy_xlml_poc_2.dag` dag ON dag.dag_id=dr.dag_id
  --WHERE dag.is_active = TRUE
  --WHERE dag.is_paused = FALSE
),
    
-- Check last trial only
last_task_status AS (
    SELECT
      ti.dag_id,
      ti.run_id,
      ti.task_id,
      ti.state,
      ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn
    FROM
      `amy_xlml_poc_2.task_instance` AS ti
      JOIN dag_runs AS dr
        ON ti.dag_id = dr.dag_id
        AND ti.run_id = dr.run_id
  ),    
-- Task-level success aggregation per run
task_status AS (
  SELECT 
    ti.dag_id, 
    ti.run_id,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN ti.state = 'success' THEN 1 ELSE 0 END) AS successful_tasks
  FROM last_task_status ti
  JOIN dag_runs dr ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
  GROUP BY ti.dag_id, ti.run_id
),

-- Run-level pass/fail status
dag_run_results AS (
  SELECT 
    dag_id, 
    run_id,
    total_tasks,
    successful_tasks,
    CASE WHEN total_tasks = successful_tasks THEN 1 ELSE 0 END AS is_passed
  FROM task_status
),

-- Number of top-level tests (first-level task groups or tasks)
top_level_tests AS (
  SELECT 
    dag_id,
    COUNT(DISTINCT SPLIT(task_id, '.')[OFFSET(0)]) AS num_tests
  FROM `amy_xlml_poc_2.task_instance`
  --WHERE dag_id IN (SELECT dag_id FROM dag_with_tag)
  GROUP BY dag_id
),

-- DAG owners cleaned (removes "airflow")
dag_cleaned_owners AS (
  SELECT
    dag_id,
    STRING_AGG(DISTINCT TRIM(part)) AS cleaned_owners
  FROM (
    SELECT
      dag_id,
      part
    FROM `amy_xlml_poc_2.dag`,
    UNNEST(SPLIT(owners, ',')) AS part
    WHERE LOWER(TRIM(part)) != 'airflow'
  )
  GROUP BY dag_id
),

-- DAG run statistic 
dag_statistic AS (    
  SELECT 
    drr.dag_id,
    dco.cleaned_owners AS dag_owner,
    COUNT(drr.run_id) AS total_runs,
    SUM(drr.is_passed) AS passed_runs,
    ROUND(SAFE_DIVIDE(SUM(drr.is_passed), COUNT(drr.run_id)) * 100, 2) AS pass_rate_percent,
    ttl.num_tests AS number_of_tests
  FROM dag_run_results drr
  LEFT JOIN dag_cleaned_owners dco ON drr.dag_id = dco.dag_id
  LEFT JOIN top_level_tests ttl ON drr.dag_id = ttl.dag_id
  GROUP BY drr.dag_id, dco.cleaned_owners, ttl.num_tests
)  

-- Final result
SELECT ds.*,dt.tags 
FROM dag_statistic ds
LEFT JOIN dag_with_tag dt ON ds.dag_id = dt.dag_id


