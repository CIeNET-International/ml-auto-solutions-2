CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status` AS
WITH
full_dag_runs AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.run_order_desc, runs.is_passed_run_order_desc,
    runs.total_tests, runs.successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
 
),
daily_dag_status AS (
  SELECT
    fdr.dag_id,
    ANY_VALUE(category) AS category,
    ANY_VALUE(accelerator) AS accelerator,
    ANY_VALUE(fdr.dag_owner) AS dag_owner,
    ANY_VALUE(fdr.tags) AS tags,
    ANY_VALUE(fdr.formatted_schedule) AS formatted_schedule,
    ANY_VALUE(fdr.is_paused) AS is_paused,
    fdr.run_date AS date,
    CASE
      WHEN COUNT(fdr.run_id) = 0 THEN 'no run'
      --WHEN COUNTIF(fdr.is_passed = 0) > 0 THEN 'failed'
      WHEN COUNTIF(fdr.is_passed = 0 AND fdr.is_partial_passed = 0) > 0 THEN 'failed'
      WHEN COUNTIF(fdr.is_partial_passed = 1) > 0 THEN 'partial'
      ELSE 'success'
    END AS status,
    SUM(fdr.total_tests) AS total_tests,
    SUM(fdr.successful_tests) AS successful_tests,
    SUM(fdr.total_tests) - SUM(fdr.successful_tests) AS failed_tests
  FROM
    full_dag_runs AS fdr
  GROUP BY
    fdr.dag_id,
    fdr.run_date
),

distinct_dag_details AS (
  SELECT DISTINCT
    dag_id,
    dag_owner,
    tags,
    category,
    accelerator,
    formatted_schedule,
    is_paused
  FROM
    daily_dag_status
),
min_max_dates AS (
  SELECT
    MIN(run_date) AS min_date,
    MAX(run_date) AS max_date
  FROM
    full_dag_runs
),
date_series AS (
  SELECT
    CAST(d AS DATE) AS date
  FROM
    UNNEST(GENERATE_DATE_ARRAY((SELECT min_date FROM min_max_dates), (SELECT max_date FROM min_max_dates))) AS d
)

SELECT
  ddd.dag_id,
  ddd.dag_owner,
  ddd.tags,
  ddd.category,
  ddd.accelerator,
  ds.date,
  COALESCE(dds.status, 'no run') AS status,
  dds.total_tests,
  dds.successful_tests,
  dds.failed_tests
FROM
  distinct_dag_details AS ddd
CROSS JOIN
  date_series AS ds
LEFT JOIN
  daily_dag_status AS dds ON ddd.dag_id = dds.dag_id AND ds.date = dds.date
  
