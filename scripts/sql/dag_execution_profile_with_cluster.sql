CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_execution_profile_with_cluster` AS
-- DAGs that have at least one mapped test
WITH qualifying_dags AS (
  SELECT DISTINCT dag_id
  FROM `amy_xlml_poc_prod.cluster_info_view_latest`
),

-- Choose reference run: last successful if present, else most recent run
last_runs AS (
  SELECT
    d.dag_id,
    COALESCE(d.last_success_run_id, lr.run_id) AS chosen_run_id,
    CASE WHEN d.last_success_run_id IS NOT NULL THEN TRUE ELSE FALSE END AS run_succ
  FROM `amy_xlml_poc_prod.dag_duration_stat` d
  LEFT JOIN (
    SELECT
      dr.dag_id,
      dr.run_id
    FROM `amy_xlml_poc_prod.dag_run` dr
    WHERE dr.start_date IS NOT NULL AND dr.end_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY execution_date DESC) = 1
  ) lr
    ON d.dag_id = lr.dag_id
  WHERE d.dag_id IN (SELECT dag_id FROM qualifying_dags) 
),

-- Reference run timing details (formatted; no millis)
referenced_runs AS (
  SELECT
    dr.dag_id,
    dr.run_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', dr.start_date) AS start_date_fmt,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S UTC', dr.end_date)   AS end_date_fmt,
    TIMESTAMP_DIFF(dr.end_date, dr.start_date, SECOND)       AS run_duration_seconds
  FROM `amy_xlml_poc_prod.dag_run` dr
),

-- Compute test start offsets from the chosen run (preserves parallelism)
task_offsets AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    TIMESTAMP_DIFF(ti.start_date, dr.start_date, SECOND) AS start_offset_seconds
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN `amy_xlml_poc_prod.dag_run` dr
    ON ti.dag_id = dr.dag_id
   AND ti.run_id = dr.run_id
),

chosen_offsets AS (
  SELECT
    lr.dag_id,
    t.test_id,
    MIN(t.start_offset_seconds) AS start_offset_seconds
  FROM last_runs lr
  JOIN task_offsets t
    ON lr.dag_id = t.dag_id
   AND lr.chosen_run_id = t.run_id
  GROUP BY lr.dag_id, t.test_id
),

-- Full test universe for qualifying DAGs (ensures tests appear even if offset missing in chosen run)
tests_in_scope AS (
  SELECT DISTINCT dag_id, test_id
  FROM `amy_xlml_poc_prod.dag_test_duration_stat`
  WHERE dag_id IN (SELECT dag_id FROM qualifying_dags)
),

-- Add duration stats (rename avg field); LEFT JOIN offsets so rows are retained
forecast AS (
  SELECT
    u.dag_id,
    u.test_id,
    co.start_offset_seconds,
    ts.avg_duration_success_seconds AS avg_successful_test_duration_seconds,
    ts.avg_duration_any_seconds AS avg_any_test_duration_seconds,
    ts.last_success_duration_seconds_tasks AS last_success_test_duration_seconds_tasks,
    ts.last_success_duration_seconds_dagrun,
    CASE
      WHEN co.start_offset_seconds IS NOT NULL AND ts.avg_duration_success_seconds IS NOT NULL
        THEN co.start_offset_seconds + ts.avg_duration_success_seconds
      ELSE NULL
    END AS end_offset_seconds
  FROM tests_in_scope u
  LEFT JOIN chosen_offsets co
    ON u.dag_id = co.dag_id
   AND u.test_id = co.test_id
  LEFT JOIN `amy_xlml_poc_prod.dag_test_duration_stat` ts
    ON u.dag_id = ts.dag_id
   AND u.test_id = ts.test_id
)

    
-- Final: include ALL tests from qualifying DAGs; attach cluster_name per (dag_id, test_id)
SELECT
  f.dag_id,
  lr.chosen_run_id AS run_id,
  f.test_id,
  dti.cluster_name,  
  dti.project_name AS cluster_project,  
  lr.run_succ,
  rr.run_duration_seconds AS referenced_run_duration_seconds,
  f.start_offset_seconds,
  f.end_offset_seconds,
  f.avg_successful_test_duration_seconds,
  f.avg_any_test_duration_seconds,  
  f.last_success_test_duration_seconds_tasks,
  f.last_success_duration_seconds_dagrun,
  rr.start_date_fmt AS referenced_run_start_date,
  rr.end_date_fmt   AS referenced_run_end_date,
FROM forecast f
JOIN last_runs lr
  ON f.dag_id = lr.dag_id
JOIN referenced_runs rr
  ON lr.dag_id = rr.dag_id
 AND lr.chosen_run_id = rr.run_id
LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` dti
  ON f.dag_id = dti.dag_id
 AND f.test_id = dti.test_id


