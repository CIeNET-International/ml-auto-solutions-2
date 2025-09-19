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

tasks_enriched AS (
  SELECT
    b.dag_id,
    b.run_id,
    b.test_id,
    ARRAY_AGG(STRUCT(
      t.task_id AS task_id,
      t.start_date AS task_start_date,
      t.end_date AS task_end_date,
      t.try_number AS try_number,
      t.state AS state
    ) ORDER BY t.start_date) AS tasks
  FROM latest_runs_tests b
  JOIN `amy_xlml_poc_prod.task_instance` t
    ON b.dag_id = t.dag_id
   AND b.run_id = t.run_id
   AND b.test_id = SPLIT(t.task_id, '.')[OFFSET(0)]
  GROUP BY b.dag_id, b.run_id, b.test_id
),

-- Build tests array and run_status from per-test rows
run_agg AS (
  SELECT
    t1.dag_id,
    t1.run_id,
    ARRAY_AGG(STRUCT(
      t1.test_id AS test_id,
      t1.test_start_date AS test_start_date,
      t1.test_end_date AS test_end_date,
      IF(t1.test_is_passed = 1, 'success', 'failed') AS test_status,
      t2.tasks AS tasks
    ) ORDER BY t1.test_start_date) AS tests,
  FROM latest_runs_tests AS t1
  JOIN tasks_enriched AS t2
    ON t1.dag_id = t2.dag_id
   AND t1.run_id = t2.run_id
   AND t1.test_id = t2.test_id
  GROUP BY t1.dag_id, t1.run_id
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
      unnested_tests.tasks,
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





