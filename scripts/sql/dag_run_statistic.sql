
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_run_statistic` AS

-- DAG runs that dag is is_active
WITH dag_runs_ended AS (
  SELECT dr.dag_id, dr.run_id, dr.execution_date, dr.start_date, dr.end_date
  FROM `amy_xlml_poc_prod.dag_run` dr
  JOIN `amy_xlml_poc_prod.dag` dag ON dag.dag_id=dr.dag_id
  --WHERE dag.is_active = TRUE
  --WHERE dag.is_paused = FALSE
  WHERE dr.start_date is not null and dr.end_date is not null
    AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
    AND dr.dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.ignore_dags`)
),
    
-- Calculate all runs for each DAG in the date range
all_dag_runs AS (
    SELECT dag_id, COUNT(run_id) AS total_runs_all
    FROM dag_runs_ended
    GROUP BY dag_id
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
      `amy_xlml_poc_prod.task_instance` AS ti
    JOIN dag_runs_ended AS dr
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
  --JOIN dag_runs_ended dr ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
  WHERE rn = 1
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

--Runs with exec info
dag_run_info AS (
  SELECT dr.*, drr.execution_date, drr.start_date, drr.end_date,
  --ROW_NUMBER() OVER (PARTITION BY dr.dag_id ORDER BY dr.run_id DESC) AS run_order    
  ROW_NUMBER() OVER (PARTITION BY drr.dag_id ORDER BY drr.start_date DESC) AS run_order    
  FROM dag_run_results dr
  JOIN dag_runs_ended drr 
  ON dr.dag_id = drr.dag_id AND dr.run_id = drr.run_id
),

last_run AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed
  FROM dag_run_info
  WHERE run_order = 1
),

last_succ AS (
SELECT
  dag_id, run_id, execution_date, start_date, end_date
FROM (
    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY start_date DESC) AS latest_successful_run_order
    FROM
      dag_run_info
    WHERE
      is_passed = 1
)
WHERE
  latest_successful_run_order = 1
),
  
ref_dag_run AS (
  SELECT dag_id,run_id from last_run
  UNION ALL
  SELECT dag_id, run_id from last_succ
), 

-- Number of top-level tests (first-level task groups or tasks)
--top_level_tests AS (
--  SELECT 
--    dag_id,
--    COUNT(DISTINCT SPLIT(task_id, '.')[OFFSET(0)]) AS num_tests
--  FROM `amy_xlml_poc_prod.task_instance`
--  WHERE dag_id IN (SELECT dag_id FROM all_dag_runs)
--  GROUP BY dag_id
--),
top_level_tests AS (
  SELECT 
    t.dag_id,
    COUNT(DISTINCT SPLIT(t.task_id, '.')[OFFSET(0)]) AS num_tests
  FROM last_task_status t
  JOIN ref_dag_run r ON t.dag_id = r.dag_id AND t.run_id = r.run_id
  -- WHERE t.rn = 1
  GROUP BY t.dag_id
),

-- DAG run statistic 
dag_statistic AS (    
  SELECT 
    drr.dag_id,
    COUNT(drr.run_id) AS total_runs,
    SUM(drr.is_passed) AS passed_runs,
    ROUND(SAFE_DIVIDE(SUM(drr.is_passed), COUNT(drr.run_id)) * 100, 2) AS pass_rate_percent,
    ttl.num_tests AS number_of_tests
  FROM dag_run_results drr
  LEFT JOIN top_level_tests ttl ON drr.dag_id = ttl.dag_id
  GROUP BY drr.dag_id, ttl.num_tests
),

dag_sch_pause AS (
  SELECT
    dag_id,
    is_paused,
    schedule_interval,
    CASE
      WHEN schedule_interval IS NULL OR schedule_interval = '' THEN schedule_interval
      WHEN STARTS_WITH(schedule_interval, '{') AND JSON_VALUE(schedule_interval, '$.type') = 'timedelta' THEN
        -- Logic for timedelta schedules
        CONCAT(
          CAST(JSON_VALUE(schedule_interval, '$.attrs.days') AS INT64), ' day, ',
          LPAD(CAST(FLOOR(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64) / 3600) AS STRING), 2, '0'),
          ':',
          LPAD(CAST(FLOOR(MOD(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64), 3600) / 60) AS STRING), 2, '0'),
          ':',
          LPAD(CAST(MOD(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64), 60) AS STRING), 2, '0')
        )
      ELSE
        -- Logic for cron expressions and other string-based schedules
        schedule_interval
    END AS formatted_schedule
  FROM
    `amy_xlml_poc_prod.dag` 
--  WHERE schedule_interval IS NOT NULL
--    AND LOWER(schedule_interval) != 'null'
--    AND is_paused = FALSE
),

dag_clusters AS (
  SELECT dag_id,ARRAY_AGG(concat(project_name, '.', cluster_name)) as clusters
  FROM `amy_xlml_poc_prod.cluster_info_view_latest`
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
    FROM `amy_xlml_poc_prod.dag`,
    UNNEST(SPLIT(owners, ',')) AS part
    WHERE LOWER(TRIM(part)) != 'airflow'
  )
  GROUP BY dag_id
),

-- DAGs with the specified tag
dag_with_tag AS (
  SELECT dt.dag_id,ARRAY_AGG(name) as tags
  FROM `amy_xlml_poc_prod.dag_tag` dt
  GROUP BY dag_id  
)

-- Final result
SELECT ds.*, 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',ls.start_date)  AS last_succ, 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',lr.start_date) AS last_exec, 
  dag.is_paused,dag.formatted_schedule,
  dc.clusters,
  dt.tags, all_runs.total_runs_all,dco.cleaned_owners AS dag_owner,
FROM dag_statistic ds
LEFT JOIN dag_cleaned_owners dco ON ds.dag_id = dco.dag_id
LEFT JOIN dag_with_tag dt ON ds.dag_id = dt.dag_id
LEFT JOIN all_dag_runs all_runs ON ds.dag_id = all_runs.dag_id
LEFT JOIN last_succ ls ON ds.dag_id = ls.dag_id
LEFT JOIN last_run lr ON ds.dag_id = lr.dag_id
LEFT JOIN dag_clusters dc ON ds.dag_id = dc.dag_id
JOIN dag_sch_pause dag ON ds.dag_id = dag.dag_id
ORDER BY ds.dag_id


