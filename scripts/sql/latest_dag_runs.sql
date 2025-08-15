CREATE OR REPLACE VIEW `amy_xlml_poc_2.latest_dag_runs` AS
-- 1) Latest run per DAG within last 180 days
WITH latest_runs AS (
  SELECT
    dr.dag_id,
    dr.run_id,
    dr.execution_date,
    dr.start_date AS run_start_date,
    dr.end_date   AS run_end_date
  FROM `amy_xlml_poc_2.dag_run` dr
  WHERE dr.execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dr.dag_id ORDER BY dr.execution_date DESC) = 1
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
  FROM `amy_xlml_poc_2.task_instance` ti
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
)

-- 5) Final output
SELECT
  lr.dag_id,
  lr.run_id,
  lr.execution_date,
  lr.run_start_date,
  lr.run_end_date,
  ra.run_status,
  ra.tests
FROM latest_runs lr
JOIN run_agg ra
  USING (dag_id, run_id);

