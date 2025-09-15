CREATE OR REPLACE VIEW `amy_xlml_poc_prod.latest_dag_runs` AS
WITH 
-- Aggregate per test_id 
latest_runs AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date run_start_date, runs.end_date run_end_date, 
    IF(runs.is_passed = 1, 'success', 'failed') AS run_status
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE run_order_desc=1
),

latest_runs_tests AS (
  SELECT base.dag_id, runs.run_id, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  WHERE run_order_desc=1
),

-- Build tests array and run_status from per-test rows
run_agg AS (
  SELECT
    dag_id,
    run_id,
    ARRAY_AGG(STRUCT(
      test_id AS test_id,
      test_start_date AS test_start_date,
      test_end_date AS test_end_date,
      IF(test_is_passed = 1, 'success', 'failed') AS test_status
    ) ORDER BY test_start_date) AS tests,
  FROM latest_runs_tests
  GROUP BY dag_id, run_id
),

-- Cluster
run_agg_with_cluster AS (
  SELECT
    t1.dag_id,
    t1.run_id,
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
    t1.run_id
)

SELECT
  ra.dag_id,
  ra.run_id,
  lr.execution_date,
  lr.run_start_date,
  lr.run_end_date,
  lr.run_status,
  ra.tests
FROM run_agg_with_cluster ra
LEFT JOIN latest_runs lr ON lr.dag_id = ra.dag_id AND lr.run_id = ra.run_id 





