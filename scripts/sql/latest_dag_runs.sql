CREATE OR REPLACE VIEW `amy_xlml_poc_prod.latest_dag_runs` AS
WITH latest_runs AS (
  SELECT
    dr.dag_id,
    dr.run_id,
    dr.execution_date,
    dr.start_date AS run_start_date,
    dr.end_date   AS run_end_date
  FROM 
    `amy_xlml_poc_prod.dag_run` dr
  WHERE 
    dr.start_date is not null and dr.end_date is not null
      AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
      AND dr.dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.ignore_dags`)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dr.dag_id ORDER BY dr.execution_date DESC) = 1
),

-- Check last trial only
last_task_status AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.task_id,
    ti.start_date,
    ti.end_date,
    ti.state,
    ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn
  FROM
    `amy_xlml_poc_prod.task_instance` AS ti
),    
    
-- 2) Filter task_instance early + extract test_id
ti_scoped AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    ti.start_date,
    ti.end_date,
    ti.state
  FROM `amy_xlml_poc_prod.task_instance` ti
  JOIN latest_runs lr
    ON ti.dag_id = lr.dag_id
   AND ti.run_id = lr.run_id
),

-- 3) Aggregate per test_id (no nested aggregates)
test_summaries AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    MIN(start_date) AS test_start_date,
    MAX(end_date)   AS test_end_date,
    COUNTIF(state != 'success') AS failed_in_test
  FROM ti_scoped
  GROUP BY dag_id, run_id, test_id
),

-- 4) Build tests array and run_status from per-test rows
run_agg AS (
  SELECT
    dag_id,
    run_id,
    ARRAY_AGG(STRUCT(
      test_id AS test_id,
      test_start_date AS test_start_date,
      test_end_date AS test_end_date,
      IF(failed_in_test = 0, 'success', 'failed') AS test_status
    ) ORDER BY test_id) AS tests,
    IF(SUM(failed_in_test) = 0, 'success', 'failed') AS run_status
  FROM test_summaries
  GROUP BY dag_id, run_id
),

-- 5) Cluster
run_agg_with_cluster AS (
  SELECT
    t1.dag_id,
    t1.run_id,
    t1.run_status,
    ARRAY_AGG(STRUCT(
      unnested_tests.test_id,
      unnested_tests.test_start_date,
      unnested_tests.test_end_date,
      unnested_tests.test_status,
      t2.project_name AS cluster_project,
      t2.cluster_name,
      t2.accelerator_type, t2.accelerator_family,
      t3.machine_families
    ) ORDER BY unnested_tests.test_id) AS tests
  FROM
    run_agg AS t1,
    UNNEST(t1.tests) AS unnested_tests
  LEFT JOIN
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t2
    ON unnested_tests.test_id = t2.test_id AND t1.dag_id = t2.dag_id
  LEFT JOIN
    `amy_xlml_poc_prod.gke_cluster_info_view` AS t3
    ON t2.project_name = t3.project_id AND t2.cluster_name = t3.cluster_name    
  GROUP BY
    t1.dag_id,
    t1.run_id,
    t1.run_status
)

-- 6) Final output
SELECT
  lr.dag_id,
  lr.run_id,
  lr.execution_date,
  lr.run_start_date,
  lr.run_end_date,
  ra.run_status,
  ra.tests
FROM latest_runs lr
JOIN run_agg_with_cluster ra
  USING (dag_id, run_id)

