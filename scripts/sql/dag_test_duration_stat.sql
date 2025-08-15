CREATE OR REPLACE VIEW `amy_xlml_poc_2.dag_test_duration_stat` AS
WITH runs AS (
  SELECT
    dag_id,
    run_id,
    execution_date,
    start_date AS run_start_date,
    end_date AS run_end_date
  FROM `amy_xlml_poc_2.dag_run`
  WHERE start_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
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
  FROM `amy_xlml_poc_2.task_instance` ti
  JOIN runs r
    ON ti.dag_id = r.dag_id AND ti.run_id = r.run_id
),
-- Last attempt per task
last_attempts AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY dag_id, run_id, task_id
        ORDER BY try_number DESC
      ) AS rn
    FROM task_attempts
  )
  WHERE rn = 1
),
-- Step 1: Aggregate per test_id per run for wall-clock durations
test_run_durations_success AS (
  SELECT
    la.dag_id,
    la.run_id,
    la.test_id,
    TIMESTAMP_DIFF(MAX(la.end_date), MIN(la.start_date), MICROSECOND) / 1000000.0 AS wall_clock_duration_seconds
  FROM last_attempts la
  JOIN (
    SELECT dag_id, run_id, test_id,
           CASE WHEN COUNTIF(state != 'success') = 0 THEN 1 ELSE 0 END AS all_success_flag
    FROM last_attempts
    GROUP BY dag_id, run_id, test_id
  ) f
    ON la.dag_id = f.dag_id AND la.run_id = f.run_id AND la.test_id = f.test_id
  WHERE f.all_success_flag = 1
  GROUP BY la.dag_id, la.run_id, la.test_id
),
test_run_durations_any AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    TIMESTAMP_DIFF(MAX(end_date), MIN(start_date), MICROSECOND) / 1000000.0 AS wall_clock_duration_seconds
  FROM task_attempts
  GROUP BY dag_id, run_id, test_id
),
-- Step 2: Per-DAG + test_id aggregated stats
test_stats AS (
  SELECT
    r.dag_id,
    tr.test_id,
    COUNT(DISTINCT s.run_id) AS success_run_count,
    COUNT(DISTINCT a.run_id) AS any_run_count,
    ROUND(AVG(s.wall_clock_duration_seconds), 0) AS avg_duration_success_seconds,
    MAX(s.wall_clock_duration_seconds) AS max_duration_success_seconds,
    ROUND(AVG(a.wall_clock_duration_seconds), 0) AS avg_duration_any_seconds,
    MAX(a.wall_clock_duration_seconds) AS max_duration_any_seconds
  FROM runs r
  LEFT JOIN (
    SELECT DISTINCT dag_id, test_id
    FROM task_attempts
  ) tr
    ON r.dag_id = tr.dag_id
  LEFT JOIN test_run_durations_success s
    ON r.dag_id = s.dag_id
    AND r.run_id = s.run_id
    AND tr.test_id = s.test_id
  LEFT JOIN test_run_durations_any a
    ON r.dag_id = a.dag_id
    AND r.run_id = a.run_id
    AND tr.test_id = a.test_id
  GROUP BY r.dag_id, tr.test_id
),
-- Step 3: Last successful run (from tasks)
last_success_run_tasks AS (
  SELECT
    t.dag_id,
    t.test_id,
    r.run_id,
    r.execution_date AS last_success_execution_date_tasks,
    MIN(la.start_date) AS last_success_start_date_tasks,
    MAX(la.end_date) AS last_success_end_date_tasks,
    TIMESTAMP_DIFF(MAX(la.end_date), MIN(la.start_date), MICROSECOND) / 1000000.0 AS last_success_duration_seconds_tasks
  FROM test_run_durations_success t
  JOIN runs r
    ON t.dag_id = r.dag_id AND t.run_id = r.run_id
  JOIN last_attempts la
    ON t.dag_id = la.dag_id AND t.run_id = la.run_id AND t.test_id = la.test_id
  GROUP BY t.dag_id, t.test_id, r.run_id, r.execution_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.dag_id, t.test_id ORDER BY r.execution_date DESC) = 1
),
-- Step 4: Last successful run (from dag_run table)
last_success_run_dagrun AS (
  SELECT
    t.dag_id,
    t.test_id,
    r.run_id,
    r.execution_date AS last_success_execution_date_dagrun,
    r.run_start_date AS last_success_start_date_dagrun,
    r.run_end_date AS last_success_end_date_dagrun,
    TIMESTAMP_DIFF(r.run_end_date, r.run_start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_dagrun
  FROM test_run_durations_success t
  JOIN runs r
    ON t.dag_id = r.dag_id AND t.run_id = r.run_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY t.dag_id, t.test_id ORDER BY r.execution_date DESC) = 1
)
SELECT
  ts.dag_id,
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
  ON ts.dag_id = lsrd.dag_id AND ts.test_id = lsrd.test_id;

