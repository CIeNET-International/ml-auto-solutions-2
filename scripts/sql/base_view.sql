
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.base_view` AS
WITH 

all_dags AS (
  SELECT * FROM `amy_xlml_poc_prod.all_dag_base_view`
),

dag_runs_ended AS (
  SELECT dr.dag_id, dr.run_id, dr.execution_date, dr.start_date, dr.end_date
  FROM `amy_xlml_poc_prod.dag_run` dr
  JOIN all_dags dag ON dag.dag_id=dr.dag_id
  --WHERE dag.is_active = TRUE
  --WHERE dag.is_paused = FALSE
  WHERE dr.start_date is not null and dr.end_date is not null
    --AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
   AND DATE(start_date,'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
   AND dr.dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.config_ignore_dags`)
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
)


SELECT
  d.dag_id,
  d.dag_owners,
  d.schedule_interval,
  REGEXP_REPLACE(d.formatted_schedule, r'^"|"$', '') AS formatted_schedule,
  d.is_paused,
  d.tags,
  d.category,
  d.accelerator,
  d.description,
  tlt.test_ids,
  cnt.total_runs,
  cnt.passed_runs,
  tlt.total_tests AS total_tests,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',le.start_date)  AS last_exec,  
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',ls.start_date)  AS last_succ, 
  ard.runs AS runs
FROM all_dags d
LEFT JOIN top_level_tests tlt ON d.dag_id = tlt.dag_id
LEFT JOIN all_run_details ard ON d.dag_id = ard.dag_id
LEFT JOIN dag_run_cnt cnt ON d.dag_id = cnt.dag_id
LEFT JOIN last_exec le ON d.dag_id = le.dag_id
LEFT JOIN last_succ ls ON d.dag_id = ls.dag_id
ORDER BY d.dag_id


