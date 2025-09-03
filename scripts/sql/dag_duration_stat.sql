CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_duration_stat` AS
WITH runs AS (
  SELECT
    dag_id,
    run_id,
    execution_date,
    start_date AS run_start_date,
    end_date AS run_end_date
  FROM `amy_xlml_poc_prod.dag_run`
  WHERE start_date is not null and end_date is not null
    AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
    AND dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.ignore_dags`)
),
dag_with_tag AS (
  SELECT dt.dag_id, ARRAY_AGG(dt.name) AS tags
  FROM `amy_xlml_poc_prod.dag_tag` dt
  GROUP BY dt.dag_id
),
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
-- Flag runs where all last attempts succeeded
run_success_flags AS (
  SELECT
    dag_id,
    run_id,
    CASE WHEN COUNTIF(state != 'success') = 0 THEN 1 ELSE 0 END AS all_success_flag
  FROM last_attempts
  GROUP BY dag_id, run_id
),
-- Wall clock durations for successful runs
dag_run_success AS (
  SELECT
    la.dag_id,
    la.run_id,
    TIMESTAMP_DIFF(MAX(la.end_date), MIN(la.start_date), MICROSECOND) / 1000000.0 AS wall_clock_duration_seconds
  FROM last_attempts la
  JOIN run_success_flags f
    ON la.dag_id = f.dag_id AND la.run_id = f.run_id
  WHERE f.all_success_flag = 1
  GROUP BY la.dag_id, la.run_id
),
-- Wall clock durations for any runs (regardless of success)
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
    r.dag_id,
    COUNT(DISTINCT s.run_id) AS success_run_count,
    COUNT(DISTINCT a.run_id) AS any_run_count,
    AVG(s.wall_clock_duration_seconds) AS avg_duration_success_seconds,
    MAX(s.wall_clock_duration_seconds) AS max_duration_success_seconds,
    AVG(a.wall_clock_duration_seconds) AS avg_duration_any_seconds,
    MAX(a.wall_clock_duration_seconds) AS max_duration_any_seconds
  FROM runs r
  LEFT JOIN dag_run_success s
    ON r.dag_id = s.dag_id AND r.run_id = s.run_id
  LEFT JOIN dag_run_any a
    ON r.dag_id = a.dag_id AND r.run_id = a.run_id
  GROUP BY r.dag_id
),
-- Last successful run (from tasks)
last_success_run_tasks AS (
  SELECT
    f.dag_id,
    r.run_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', r.execution_date) AS last_success_execution_date_tasks,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MIN(la.start_date)) AS last_success_start_date_tasks,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', MAX(la.end_date)) AS last_success_end_date_tasks,
    TIMESTAMP_DIFF(MAX(la.end_date), MIN(la.start_date), MICROSECOND) / 1000000.0 AS last_success_duration_seconds_tasks
  FROM run_success_flags f
  JOIN runs r
    ON f.dag_id = r.dag_id AND f.run_id = r.run_id
  JOIN last_attempts la
    ON f.dag_id = la.dag_id AND f.run_id = la.run_id
  WHERE f.all_success_flag = 1
  GROUP BY f.dag_id, r.run_id, r.execution_date
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.dag_id ORDER BY r.execution_date DESC) = 1
),
-- Last successful run (from dag_run table)
last_success_run_dagrun AS (
  SELECT
    f.dag_id,
    r.run_id AS last_success_run_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', r.execution_date) AS last_success_execution_date_dagrun,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', r.run_start_date) AS last_success_start_date_dagrun,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', r.run_end_date) AS last_success_end_date_dagrun,
    TIMESTAMP_DIFF(r.run_end_date, r.run_start_date, MICROSECOND) / 1000000.0 AS last_success_duration_seconds_dagrun
  FROM run_success_flags f
  JOIN runs r
    ON f.dag_id = r.dag_id AND f.run_id = r.run_id
  WHERE f.all_success_flag = 1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY f.dag_id ORDER BY r.execution_date DESC) = 1
)
SELECT
  ds.dag_id,
  d.cleaned_owners owners,
  dag_with_tag.tags,    
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
LEFT JOIN dag_cleaned_owners d
  ON ds.dag_id = d.dag_id
LEFT JOIN dag_with_tag 
  ON ds.dag_id = dag_with_tag.dag_id    
LEFT JOIN last_success_run_tasks lsrt
  ON ds.dag_id = lsrt.dag_id
LEFT JOIN last_success_run_dagrun lsrd
  ON ds.dag_id = lsrd.dag_id;

