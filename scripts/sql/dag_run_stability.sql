CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_stability` AS
WITH
dag_runs_stat AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_stat' AS data_type, 
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests, runs.successful_tests, runs.total_tests + runs.total_tests_q AS total_run_tests, runs.run_order_desc
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
    WHERE runs.run_order_desc <= 7
),

dag_statistic AS (
  SELECT dag_id,
    SUM(is_passed) AS passed,
    SUM(is_partial_passed) AS partial,
    COUNT(DISTINCT run_id) AS total  
  FROM dag_runs_stat 
  GROUP by dag_id
),
dag_stability AS (
  SELECT s.*,
    CASE
      WHEN passed = total THEN 'Stable'
      WHEN passed + partial > 0 THEN 'Unstable'
     ELSE 'Broken'
    END AS stability
  FROM
    dag_statistic s
),
dag_counts AS (
  SELECT
    (SELECT COUNT(DISTINCT dag_id) FROM `cienet-cmcs.amy_xlml_poc_prod.dag_run_status`) AS total_dags,
    (SELECT COUNT(DISTINCT dag_id) FROM dag_statistic) AS run_dags,   
),
not_run AS (
  SELECT 
    'Not Run' AS stability,
    total_dags - run_dags AS dags
  FROM dag_counts
)


SELECT stability, COUNT(dag_id) AS dags 
FROM dag_stability
GROUP BY stability
UNION ALL
SELECT * FROM not_run

