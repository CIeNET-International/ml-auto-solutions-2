CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_duration_stat` AS

WITH all_runs AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
), 

succ_runs AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE runs.is_passed=1
), 

all_tests AS (
  SELECT base.dag_id, runs.run_id,runs.execution_date,  runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
), 

succ_runs_tests AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE runs.is_passed=1
), 

last_succ_runs_tests AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE runs.is_passed=1 and is_passed_run_order_desc=1
), 

dag_static AS (
  SELECT dag_id, dag_owners, tags, category, accelerator
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

-- All attempts for all tasks
task_attempts AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.task_id,
    ti.try_number,
    ti.state,
    ti.start_date,
    ti.end_date
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN all_runs r
    ON ti.dag_id = r.dag_id AND ti.run_id = r.run_id
),

-- Wall clock durations for successful runs
dag_run_success AS (
  SELECT
    la.dag_id,
    la.run_id,
    TIMESTAMP_DIFF(MAX(la.end_date), MIN(la.start_date), MICROSECOND) / 1000000.0 AS wall_clock_duration_seconds
  FROM succ_runs la
  GROUP BY la.dag_id, la.run_id
),
-- Wall clock durations for any runs (regardless of success)
--dag_run_any AS (
--  SELECT
--    dag_id,
--    run_id,
--    TIMESTAMP_DIFF(MAX(end_date), MIN(start_date), SECOND) AS wall_clock_duration_seconds
--  FROM all_runs
--  GROUP BY dag_id, run_id
--),

dag_run_any AS (
  SELECT
    dag_id,
    run_id,
    TIMESTAMP_DIFF(MAX(end_date), MIN(start_date), SECOND) AS wall_clock_duration_seconds
  FROM task_attempts
  GROUP BY dag_id, run_id
),

-- Per-DAG aggregated stats
dag_stats AS (
  SELECT
    a.dag_id,
    COUNT(DISTINCT s.run_id) AS success_run_count,
    COUNT(DISTINCT a.run_id) AS any_run_count,
    AVG(s.wall_clock_duration_seconds) AS avg_duration_success_seconds,
    MAX(s.wall_clock_duration_seconds) AS max_duration_success_seconds,
    AVG(a.wall_clock_duration_seconds) AS avg_duration_any_seconds,
    MAX(a.wall_clock_duration_seconds) AS max_duration_any_seconds
  FROM dag_run_any a
  LEFT JOIN dag_run_success s
    ON a.dag_id = s.dag_id AND a.run_id = s.run_id
  GROUP BY a.dag_id
),

-- Last successful run (from tasks)
last_success_run_tasks AS (
  SELECT
    dag_id,
    run_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', ANY_VALUE(execution_date)) AS last_success_execution_date_tasks,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MIN(test_start_date)) AS last_success_start_date_tasks,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MAX(test_end_date)) AS last_success_end_date_tasks,
    TIMESTAMP_DIFF(MAX(test_end_date), MIN(test_start_date), MICROSECOND) / 1000000.0 AS last_success_duration_seconds_tasks
  FROM last_succ_runs_tests 
  GROUP BY dag_id, run_id
),

-- Last successful run (from dag_run table)
last_success_run_dagrun AS (
  SELECT
    dag_id,
    run_id AS last_success_run_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', execution_date) AS last_success_execution_date_dagrun,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', start_date) AS last_success_start_date_dagrun,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', end_date) AS last_success_end_date_dagrun,
    TIMESTAMP_DIFF(end_date, start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_dagrun
  FROM succ_runs
  WHERE is_passed_run_order_desc=1
)
SELECT
  ds.dag_id,
  d.dag_owners owners,
  d.tags, category, accelerator,    
  ds.success_run_count,
  ds.any_run_count,
  round(ds.avg_duration_success_seconds, 0) AS avg_duration_success_seconds,
  round(ds.max_duration_success_seconds, 0) AS max_duration_success_seconds,
  round(ds.avg_duration_any_seconds, 0) AS avg_duration_any_seconds,
  round(ds.max_duration_any_seconds, 0) AS max_duration_any_seconds,
  lsrt.last_success_execution_date_tasks,
  lsrt.last_success_start_date_tasks,
  lsrt.last_success_end_date_tasks,
  round(lsrt.last_success_duration_seconds_tasks, 0) AS last_success_duration_seconds_tasks,
  lsrd.last_success_run_id,   
  lsrd.last_success_execution_date_dagrun,
  lsrd.last_success_start_date_dagrun,
  lsrd.last_success_end_date_dagrun,
  round(lsrd.last_success_duration_seconds_dagrun, 0) AS last_success_duration_seconds_dagrun
FROM dag_stats ds
LEFT JOIN dag_static d
  ON ds.dag_id = d.dag_id
LEFT JOIN last_success_run_tasks lsrt
  ON ds.dag_id = lsrt.dag_id
LEFT JOIN last_success_run_dagrun lsrd
  ON ds.dag_id = lsrd.dag_id;

