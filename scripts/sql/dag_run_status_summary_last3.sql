CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_run_status_summary_last3` AS 
WITH
filtered_runs_r AS (
  SELECT runs.run_order_desc, base.category, base.accelerator, base.dag_id, base.tags, base.total_runs, base.total_tests, base.formatted_schedule,
    DATE(runs.start_date) AS run_date, runs.is_passed, runs.is_partial_passed, runs.total_tests AS run_total_tests, runs.successful_tests AS run_successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs 
  WHERE runs.run_order_desc <= 3
),
filtered_runs_qr AS (
  SELECT runs.run_order_desc, base.category, base.accelerator, base.dag_id, base.tags, base.total_runs, base.total_tests, base.formatted_schedule,
    DATE(runs.start_date) AS run_date, runs.is_passed, runs.is_partial_passed, runs.total_tests AS run_total_tests, runs.successful_tests AS run_successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs_qr) AS runs 
  WHERE runs.run_order_desc <= 3
),
status AS (
  SELECT run_order_desc,SUM(is_passed) AS passed_runs, SUM(is_partial_passed) AS partial_passed_runs,COUNT(DISTINCT dag_id) AS total_runs
  FROM filtered_runs_r
  GROUP BY run_order_desc
),
qr AS (
  SELECT run_order_desc, COUNT(DISTINCT dag_id) AS total_runs_qr
  FROM filtered_runs_qr
  GROUP BY run_order_desc
)

SELECT s.*,qr.total_runs_qr 
FROM status s
LEFT JOIN qr ON s.run_order_desc = qr.run_order_desc

