CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_test_duration_stat_iq` AS
WITH
all_runs_pre AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, FALSE AS is_quarantined_dag 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs
), 
all_runs_qr AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, TRUE AS is_quarantined_dag
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs_qr) AS runs
), 
all_runs AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag FROM all_runs_pre
  UNION ALL
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag FROM all_runs_qr
), 

succ_runs AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag 
  FROM all_runs
  WHERE is_passed=1
), 

all_tests_pre AS (
  SELECT base.dag_id, runs.run_id,runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, FALSE AS is_quarantined_dag,
    tests.test_id,tests.start_date test_start_date, tests.end_date test_end_date, tests.is_passed AS test_is_passed, FALSE AS is_quarantined_test
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
), 
all_tests_qr AS (
  SELECT base.dag_id, runs.run_id,runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, TRUE AS is_quarantined_dag,
    tests.test_id,tests.start_date test_start_date, tests.end_date test_end_date, tests.is_passed AS test_is_passed, TRUE AS is_quarantined_test
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
), 
all_tests_qt AS (
  SELECT base.dag_id, runs.run_id,runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, FALSE AS is_quarantined_dag,
    tests.test_id,tests.start_date test_start_date, tests.end_date test_end_date, tests.is_passed AS test_is_passed, TRUE AS is_quarantined_test
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs
  JOIN UNNEST (runs.tests_q) AS tests
), 

all_tests AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag, 
    test_id, test_start_date, test_end_date, test_is_passed, is_quarantined_test
  FROM all_tests_pre
  UNION ALL
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag, 
    test_id, test_start_date, test_end_date, test_is_passed, is_quarantined_test
  FROM all_tests_qr
  UNION ALL
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag, 
    test_id, test_start_date, test_end_date, test_is_passed, is_quarantined_test
  FROM all_tests_qt  
), 

succ_runs_tests AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag,
    test_id, test_start_date, test_end_date, is_quarantined_test
  FROM all_tests
  WHERE is_passed=1
), 

succ_tests_tests AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag,
    test_id, test_start_date, test_end_date, is_quarantined_test
  FROM all_tests
  WHERE test_is_passed=1
), 
fail_tests_tests AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag,
    test_id, test_start_date, test_end_date, is_quarantined_test
  FROM all_tests
  WHERE test_is_passed=0
), 

last_succ_runs_tests AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date, is_passed, run_order_desc, is_passed_run_order_desc, is_quarantined_dag,
    test_id, test_start_date, test_end_date, is_quarantined_test
  FROM all_tests
  WHERE is_passed=1 and is_passed_run_order_desc=1
), 

dag_static AS (
  SELECT dag_id, dag_owners, tags, category, accelerator, formatted_schedule
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

-- All attempts for all tasks with test_id extracted
task_attempts_instance AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    r.is_passed, r.is_passed_run_order_desc,
    r.execution_date, r.start_date, r.end_date,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.task_id,
    --ti.try_number,
    --ti.state,
    ti.start_date AS test_start_date,
    ti.end_date AS test_end_date
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN all_runs r
    ON ti.dag_id = r.dag_id AND ti.run_id = r.run_id
),
task_attempts_fail AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    r.is_passed, r.is_passed_run_order_desc,
    r.execution_date, r.start_date, r.end_date,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.task_id,
    ti.start_date AS test_start_date,
    ti.end_date AS test_end_date
  FROM `amy_xlml_poc_prod.task_fail` ti
  JOIN all_runs r
    ON ti.dag_id = r.dag_id AND ti.run_id = r.run_id
),
task_attempts_pre AS (
  SELECT * FROM task_attempts_instance
  UNION ALL
  SELECT * FROM task_attempts_fail
),
task_attempts AS (
  SELECT dag_id, run_id, test_id, 
    ANY_VALUE(is_passed) AS is_passed, 
    ANY_VALUE(is_passed_run_order_desc) AS is_passed_run_order_desc,
    ANY_VALUE(execution_date) AS execution_date, 
    ANY_VALUE(start_date) AS start_date, 
    ANY_VALUE(end_date) AS end_date,
    MIN(test_start_date) AS test_start_date, 
    MAX(test_end_date) AS test_end_date,
    TIMESTAMP_DIFF(MAX(test_end_date), MIN(test_start_date), MICROSECOND) / 1000000.0 AS test_duration_seconds
  FROM task_attempts_pre
  GROUP BY dag_id, run_id, test_id
),
task_attempts_test_state AS (
  SELECT a.*, t.test_is_passed
  FROM task_attempts a
  JOIN all_tests t 
    ON a.dag_id = t.dag_id AND a.run_id = t.run_id AND a.test_id = t.test_id
),

-- Aggregate per test_id per run for wall-clock durations
test_run_durations_success AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(test_duration_seconds) AS wall_clock_duration_seconds
  --FROM succ_runs_tests 
  FROM task_attempts 
  WHERE is_passed = 1
  GROUP BY dag_id, run_id, test_id
),
test_run_durations_success_test_state AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(test_duration_seconds) AS wall_clock_duration_seconds
  --FROM succ_runs_tests 
  FROM task_attempts_test_state 
  WHERE test_is_passed = 1
  GROUP BY dag_id, run_id, test_id
),
test_run_durations_failed_test_state AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(test_duration_seconds) AS wall_clock_duration_seconds
  --FROM succ_runs_tests 
  FROM task_attempts_test_state 
  WHERE test_is_passed = 0
  GROUP BY dag_id, run_id, test_id
),
test_run_durations_any AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    AVG(test_duration_seconds) AS wall_clock_duration_seconds
  --FROM all_tests 
  FROM task_attempts 
  GROUP BY dag_id, run_id, test_id
),

-- Per-DAG + test_id aggregated stats
test_stats AS (
  SELECT
    r.dag_id,
    r.test_id,
    COUNT(DISTINCT s.run_id) AS success_run_count,
    COUNT(DISTINCT r.run_id) AS any_run_count,
    COUNT(DISTINCT t.run_id) AS success_test_count,
    AVG(s.wall_clock_duration_seconds) AS avg_duration_success_seconds,
    MAX(s.wall_clock_duration_seconds) AS max_duration_success_seconds,
    MIN(s.wall_clock_duration_seconds) AS min_duration_success_seconds,
    AVG(t.wall_clock_duration_seconds) AS avg_duration_test_success_seconds,
    MAX(t.wall_clock_duration_seconds) AS max_duration_test_success_seconds,
    MIN(t.wall_clock_duration_seconds) AS min_duration_test_success_seconds,    
    AVG(tf.wall_clock_duration_seconds) AS avg_duration_test_failed_seconds,
    MAX(tf.wall_clock_duration_seconds) AS max_duration_test_failed_seconds,
    MIN(tf.wall_clock_duration_seconds) AS min_duration_test_failed_seconds,    
    AVG(a.wall_clock_duration_seconds) AS avg_duration_any_seconds,
    MAX(a.wall_clock_duration_seconds) AS max_duration_any_seconds,
    MIN(a.wall_clock_duration_seconds) AS min_duration_any_seconds,   
  FROM task_attempts r
  LEFT JOIN test_run_durations_success s
    ON r.dag_id = s.dag_id
    AND r.run_id = s.run_id
    AND r.test_id = s.test_id
  LEFT JOIN test_run_durations_success_test_state t
    ON r.dag_id = t.dag_id
    AND r.run_id = t.run_id
    AND r.test_id = t.test_id    
  LEFT JOIN test_run_durations_failed_test_state tf
    ON r.dag_id = tf.dag_id
    AND r.run_id = tf.run_id
    AND r.test_id = tf.test_id    
  LEFT JOIN test_run_durations_any a
    ON r.dag_id = a.dag_id
    AND r.run_id = a.run_id
    AND r.test_id = a.test_id
  GROUP BY r.dag_id, r.test_id
),

-- Last successful run (from tasks)
last_success_run_tasks AS (  
  SELECT
    dag_id,
    run_id,
    test_id,
    execution_date AS last_success_execution_date_tasks,
    test_start_date AS last_success_start_date_tasks,
    test_end_date AS last_success_end_date_tasks,
    test_duration_seconds AS last_success_duration_seconds_tasks
  FROM task_attempts t
  WHERE is_passed = 1 AND is_passed_run_order_desc = 1
),
-- Last successful run (from dag_run table)
last_success_run_dagrun AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    execution_date AS last_success_execution_date_dagrun,
    start_date AS last_success_start_date_dagrun,
    end_date AS last_success_end_date_dagrun,
    TIMESTAMP_DIFF(end_date, start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_dagrun
  FROM task_attempts t
  WHERE is_passed = 1 AND is_passed_run_order_desc = 1
),

last_success_tests AS (  
  SELECT
    dag_id,
    test_id,
    start_date AS last_test_success_run_start_date,
    test_duration_seconds AS last_test_succuss_test_duration_seconds
  FROM task_attempts_test_state t
  WHERE test_is_passed = 1
  QUALIFY ROW_NUMBER() OVER(PARTITION BY dag_id, test_id ORDER BY start_date DESC) = 1
),

dag_is_qr AS (
  SELECT dag_id, ANY_VALUE(is_quarantined_dag) AS is_quarantined_dag
  FROM all_runs
  GROUP BY dag_id
),
test_is_qr AS (
  SELECT dag_id, test_id, ANY_VALUE(is_quarantined_test) AS is_quarantined_test
  FROM all_tests
  GROUP BY dag_id,test_id
)

SELECT
  ts.dag_id,
  d.dag_owners,
  d.tags,
  d.category,
  d.accelerator,
  d.formatted_schedule,
  ts.test_id,
  ts.success_run_count,
  ts.any_run_count,
  ts.success_test_count,
  ROUND(ts.avg_duration_success_seconds, 0) AS avg_duration_success_seconds,
  ROUND(ts.max_duration_success_seconds, 0) AS max_duration_success_seconds,
  ROUND(ts.min_duration_success_seconds, 0) AS min_duration_success_seconds,
  ROUND(ts.avg_duration_test_success_seconds, 0) AS avg_duration_test_success_seconds,
  ROUND(ts.max_duration_test_success_seconds, 0) AS max_duration_test_success_seconds,
  ROUND(ts.min_duration_test_success_seconds, 0) AS min_duration_test_success_seconds,
  ROUND(ts.avg_duration_test_failed_seconds, 0) AS avg_duration_test_failed_seconds,
  ROUND(ts.max_duration_test_failed_seconds, 0) AS max_duration_test_failed_seconds,
  ROUND(ts.min_duration_test_failed_seconds, 0) AS min_duration_test_failed_seconds,
  ROUND(ts.avg_duration_any_seconds, 0) AS avg_duration_any_seconds,
  ROUND(ts.max_duration_any_seconds, 0) AS max_duration_any_seconds,
  ROUND(ts.min_duration_any_seconds, 0) AS min_duration_any_seconds,
  lsrt.last_success_execution_date_tasks,
  lsrt.last_success_start_date_tasks,
  lsrt.last_success_end_date_tasks,
  ROUND(lsrt.last_success_duration_seconds_tasks, 0) AS last_success_duration_seconds_tasks,
  lsrd.last_success_execution_date_dagrun,
  lsrd.last_success_start_date_dagrun,
  lsrd.last_success_end_date_dagrun,
  ROUND(lsrd.last_success_duration_seconds_dagrun, 0) AS last_success_duration_seconds_dagrun,
  ar.is_quarantined_dag,
  qt.is_quarantined_test,
  lst.last_test_success_run_start_date,
  lst.last_test_succuss_test_duration_seconds
FROM test_stats ts
LEFT JOIN last_success_run_tasks lsrt
  ON ts.dag_id = lsrt.dag_id AND ts.test_id = lsrt.test_id
LEFT JOIN last_success_run_dagrun lsrd
  ON ts.dag_id = lsrd.dag_id AND ts.test_id = lsrd.test_id
LEFT JOIN dag_static d ON ts.dag_id = d.dag_id
JOIN dag_is_qr ar ON ts.dag_id = ar.dag_id  
JOIN test_is_qr qt ON ts.dag_id = qt.dag_id AND ts.test_id = qt.test_id
LEFT JOIN last_success_tests lst ON ts.dag_id = lst.dag_id AND ts.test_id = lst.test_id


