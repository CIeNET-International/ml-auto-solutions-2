CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_test_duration_stat` AS
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
  tests.test_id, tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
), 

succ_runs_tests AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.test_id, tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE runs.is_passed=1
), 

last_succ_runs_tests AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
  tests.test_id, tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE runs.is_passed=1 and is_passed_run_order_desc=1
), 

dag_static AS (
  SELECT dag_id, dag_owners, tags, category, accelerator
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

-- All attempts for all tasks with test_id extracted
task_attempts AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.task_id,
    ti.try_number,
    ti.state,
    ti.start_date,
    ti.end_date
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN all_runs r
    ON ti.dag_id = r.dag_id AND ti.run_id = r.run_id
),

-- Step 1: Aggregate per test_id per run for wall-clock durations
test_run_durations_success AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(TIMESTAMP_DIFF(test_end_date, test_start_date, MICROSECOND) / 1000000.0) AS wall_clock_duration_seconds
  FROM succ_runs_tests 
  GROUP BY dag_id, run_id, test_id
),

test_run_durations_any AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(TIMESTAMP_DIFF(test_end_date, test_start_date, MICROSECOND) / 1000000.0) AS wall_clock_duration_seconds
  FROM all_tests 
  GROUP BY dag_id, run_id, test_id
),

-- Step 2: Per-DAG + test_id aggregated stats
test_stats AS (
  SELECT
    r.dag_id,
    r.test_id,
    COUNT(DISTINCT s.run_id) AS success_run_count,
    COUNT(DISTINCT r.run_id) AS any_run_count,
    ROUND(AVG(s.wall_clock_duration_seconds), 0) AS avg_duration_success_seconds,
    MAX(s.wall_clock_duration_seconds) AS max_duration_success_seconds,
    ROUND(AVG(a.wall_clock_duration_seconds), 0) AS avg_duration_any_seconds,
    MAX(a.wall_clock_duration_seconds) AS max_duration_any_seconds
  FROM all_tests r
  LEFT JOIN test_run_durations_success s
    ON r.dag_id = s.dag_id
    AND r.run_id = s.run_id
    AND r.test_id = s.test_id
  LEFT JOIN test_run_durations_any a
    ON r.dag_id = a.dag_id
    AND r.run_id = a.run_id
    AND r.test_id = a.test_id
  GROUP BY r.dag_id, r.test_id
),

-- Step 3: Last successful run (from tasks)
last_success_run_tasks AS (
  SELECT
    dag_id,
    test_id,
    run_id,
    execution_date AS last_success_execution_date_tasks,
    test_start_date AS last_success_start_date_tasks,
    test_end_date AS last_success_end_date_tasks,
    TIMESTAMP_DIFF(test_end_date, test_start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_tasks
  FROM last_succ_runs_tests t
),
-- Step 4: Last successful run (from dag_run table)
last_success_run_dagrun AS (
  SELECT
    dag_id,
    test_id,
    run_id,
    execution_date AS last_success_execution_date_dagrun,
    start_date AS last_success_start_date_dagrun,
    end_date AS last_success_end_date_dagrun,
    TIMESTAMP_DIFF(end_date, start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_dagrun
  FROM succ_runs_tests
  WHERE is_passed_run_order_desc=1
)


SELECT
  ts.dag_id,
  d.dag_owners,
  d.tags,
  d.category,
  d.accelerator,
  ts.test_id,
  ts.success_run_count,
  ts.any_run_count,
  ts.avg_duration_success_seconds,
  ts.max_duration_success_seconds,
  ts.avg_duration_any_seconds,
  ts.max_duration_any_seconds,
  lsrt.last_success_execution_date_tasks,
  lsrt.last_success_start_date_tasks,
  lsrt.last_success_end_date_tasks,
  ROUND(lsrt.last_success_duration_seconds_tasks, 0) AS last_success_duration_seconds_tasks,
  lsrd.last_success_execution_date_dagrun,
  lsrd.last_success_start_date_dagrun,
  lsrd.last_success_end_date_dagrun,
  ROUND(lsrd.last_success_duration_seconds_dagrun, 0) AS last_success_duration_seconds_dagrun
FROM test_stats ts
LEFT JOIN last_success_run_tasks lsrt
  ON ts.dag_id = lsrt.dag_id AND ts.test_id = lsrt.test_id
LEFT JOIN last_success_run_dagrun lsrd
  ON ts.dag_id = lsrd.dag_id AND ts.test_id = lsrd.test_id
LEFT JOIN dag_static d ON ts.dag_id = d.dag_id

