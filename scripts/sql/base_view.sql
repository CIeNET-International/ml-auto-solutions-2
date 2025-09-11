
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.base_view` AS
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
    
-- Check last trial only
last_task_status AS (
    SELECT
      ti.dag_id,
      ti.run_id,
      ti.task_id,
      ti.state,
      ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn,
      ti.start_date,
      ti.end_date
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
  WHERE rn = 1
  GROUP BY ti.dag_id, ti.run_id
),

-- Test-level success aggregation per run
tasks_test AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.task_id,
    ti.state,
    ti.start_date,
    ti.end_date
  FROM last_task_status AS ti
  WHERE rn = 1
),

task_test_status AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) AS successful_tasks,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date
  FROM tasks_test
  GROUP BY dag_id, run_id, test_id
),

test_status AS (
  SELECT
    dag_id,
    run_id,
    COUNT(*) AS total_tests,
    SUM(CASE WHEN total_tasks = successful_tasks THEN 1 ELSE 0 END) AS successful_tests
  FROM task_test_status
  GROUP BY dag_id, run_id  
),

-- Run-level pass/fail status
dag_run_base1 AS (
  SELECT 
    t1.dag_id, 
    t1.run_id,
    t3.execution_date,
    t3.start_date,
    t3.end_date,
    ROW_NUMBER() OVER (PARTITION BY t3.dag_id ORDER BY t3.start_date DESC) AS run_order_desc, 
    t2.total_tests,
    t2.successful_tests,
    t1.total_tasks,
    t1.successful_tasks,
    CASE WHEN total_tasks = successful_tasks THEN 1 ELSE 0 END AS is_passed
  FROM task_status t1
  JOIN test_status t2 ON t1.dag_id = t2.dag_id AND t1.run_id = t2.run_id
  JOIN dag_runs_ended t3 ON t1.dag_id = t3.dag_id AND t1.run_id = t3.run_id
),

dag_run_base2 AS (
  SELECT
    dag_id, 
    run_id,
    execution_date,
    start_date,
    end_date,
    is_passed,
    total_tasks,
    successful_tasks,
    total_tests,
    successful_tests,
    run_order_desc,
    ROW_NUMBER() OVER (PARTITION BY dag_id, is_passed ORDER BY run_order_desc) AS is_passed_run_order_desc
  FROM dag_run_base1
),

dag_run_cnt AS (
  SELECT 
    dag_id, 
    COUNT(DISTINCT run_id) AS total_runs,
    SUM(is_passed) AS passed_runs
  FROM dag_run_base2
  GROUP BY dag_id
),

last_exec AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date
  FROM dag_run_base2
  WHERE run_order_desc=1
),

last_succ AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date
  FROM dag_run_base2
  WHERE is_passed=1 AND is_passed_run_order_desc=1
),

ref_dag_run AS (
  SELECT dag_id,run_id 
  FROM dag_run_base2 
  WHERE run_order_desc=1 OR (is_passed=1 AND is_passed_run_order_desc=1)
), 

top_level_tests AS (
  SELECT 
    t.dag_id,
    COUNT(DISTINCT test_id) AS total_tests,
    ARRAY_AGG(DISTINCT test_id) AS test_ids
  FROM task_test_status t
  JOIN ref_dag_run r ON t.dag_id = r.dag_id AND t.run_id = r.run_id
  GROUP BY t.dag_id
),

-- Aggregates test details into a nested array for each run
test_details_per_run AS (
  SELECT
    dag_id,
    run_id,
    ARRAY_AGG(STRUCT(test_id, total_tasks AS test_total_tasks, successful_tasks AS test_successful_tasks, start_date, end_date,
     CASE WHEN total_tasks = successful_tasks THEN 1 ELSE 0 END AS is_passed)) AS tests
  FROM task_test_status
  GROUP BY dag_id, run_id
),

-- Aggregates run details into an array of structs, including the nested tests
all_run_details AS (
  SELECT
    drb.dag_id,
    ARRAY_AGG(
      STRUCT(
        drb.run_id,
        drb.execution_date,
        drb.start_date,
        drb.end_date,
        drb.is_passed,
        drb.total_tests,
        drb.successful_tests,
        drb.total_tasks,
        drb.successful_tasks,
        tdpr.tests AS tests,
        drb.run_order_desc,
        drb.is_passed_run_order_desc
      ) ORDER BY drb.run_order_desc
    ) AS runs
  FROM dag_run_base2 drb
  JOIN test_details_per_run tdpr ON drb.dag_id = tdpr.dag_id AND drb.run_id = tdpr.run_id
  GROUP BY drb.dag_id
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
  WHERE dag_id in (SELECT DISTINCT dag_id FROM dag_runs_ended)  
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
),

project_match AS (
  SELECT
    d.dag_id,
    c.name AS category,
    c.pri_order,
    ROW_NUMBER() OVER (PARTITION BY d.dag_id ORDER BY c.pri_order) AS rn
  FROM dag_with_tag d
  LEFT JOIN `amy_xlml_poc_prod.config_project` c
    ON EXISTS ( -- This is the problem line
      SELECT 1
      FROM UNNEST(d.tags) t
      JOIN UNNEST(c.tag_names) ct
      ON t = ct
    )
),

-- Create a list of all dag_id and project category matches
project_matches AS (
  SELECT
    d.dag_id,
    c.name AS category,
    c.pri_order,
    ROW_NUMBER() OVER (PARTITION BY d.dag_id ORDER BY c.pri_order) AS rn
  FROM
    dag_with_tag d
  CROSS JOIN
    `amy_xlml_poc_prod.config_project` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    tag1 = tag2
),

-- Create a list of all dag_id and accelerator matches
accel_matches AS (
  SELECT
    d.dag_id,
    c.name AS accelerator,
    c.pri_order,
    ROW_NUMBER() OVER (PARTITION BY d.dag_id ORDER BY c.pri_order) AS rn
  FROM
    dag_with_tag d
  CROSS JOIN
    `amy_xlml_poc_prod.config_accelerator` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    tag1 = tag2
),

dag_with_tag_category AS (
  SELECT
    d.dag_id,
    d.tags,
    p.category,
    a.accelerator
  FROM dag_with_tag d
  LEFT JOIN project_matches p ON d.dag_id = p.dag_id AND p.rn = 1
  LEFT JOIN accel_matches a ON d.dag_id = a.dag_id AND a.rn = 1
)


SELECT
  dsp.dag_id,
  dco.cleaned_owners AS dag_owners,
  dsp.schedule_interval,
  dsp.formatted_schedule AS formatted_schedule,
  dsp.is_paused AS is_paused,
  dwt.tags AS tags,
  dwt.category AS category,
  dwt.accelerator AS accelerator,
  tlt.test_ids,
  cnt.total_runs,
  cnt.passed_runs,
  tlt.total_tests AS total_tests,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',le.start_date)  AS last_exec,  
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',ls.start_date)  AS last_succ, 
  ard.runs AS runs
FROM dag_sch_pause dsp
LEFT JOIN dag_cleaned_owners dco ON dsp.dag_id = dco.dag_id
LEFT JOIN dag_with_tag_category dwt ON dsp.dag_id = dwt.dag_id
LEFT JOIN top_level_tests tlt ON dsp.dag_id = tlt.dag_id
LEFT JOIN all_run_details ard ON dsp.dag_id = ard.dag_id
LEFT JOIN dag_run_cnt cnt ON dsp.dag_id = cnt.dag_id
LEFT JOIN last_exec le ON dsp.dag_id = le.dag_id
LEFT JOIN last_succ ls ON dsp.dag_id = ls.dag_id
ORDER BY dsp.dag_id


