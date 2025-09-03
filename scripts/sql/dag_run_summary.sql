CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_run_summary` AS

WITH  
-- DAG owners cleaned (removes "airflow")
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

-- DAGs with the specified tag
dag_with_tag AS (
  SELECT dt.dag_id,ARRAY_AGG(name) as tags
  FROM `amy_xlml_poc_prod.dag_tag` dt
  GROUP BY dag_id  
)
    

-- This view provides a summary of DAG run statistics, combining data from
-- various pre-existing materialized views for different timeframes (last 1, 3, 7 days/runs).
-- It joins the base statistics with time-series statistics to create a single, comprehensive view.

SELECT
  ds.dag_id,
  
  -- Overall Statistics (from the base view)
  ds.total_runs AS total_runs_all_time,
  ds.passed_runs AS passed_runs_all_time,
  ds.pass_rate_percent AS pass_rate_all_time,
  ds.number_of_tests AS number_of_tests,
  dco.cleaned_owners AS dag_owner,
  dt.tags,
  
  -- Statistics for the last 1 run
  ds1r.total_runs AS total_runs_last_1_run,
  ds1r.passed_runs AS passed_runs_last_1_run,
  ds1r.pass_rate_percent AS pass_rate_last_1_run,

  -- Statistics for the last 3 runs
  ds3r.total_runs AS total_runs_last_3_runs,
  ds3r.passed_runs AS passed_runs_last_3_runs,
  ds3r.pass_rate_percent AS pass_rate_last_3_runs,

  -- Statistics for the last 7 runs
  ds7r.total_runs AS total_runs_last_7_runs,
  ds7r.passed_runs AS passed_runs_last_7_runs,
  ds7r.pass_rate_percent AS pass_rate_last_7_runs,

  -- Statistics for the last 1 day
  ds1d.total_runs AS total_runs_last_1_day,
  ds1d.passed_runs AS passed_runs_last_1_day,
  ds1d.pass_rate_percent AS pass_rate_last_1_day,
  
  -- Statistics for the last 3 days
  ds3d.total_runs AS total_runs_last_3_days,
  ds3d.passed_runs AS passed_runs_last_3_days,
  ds3d.pass_rate_percent AS pass_rate_last_3_days,
  
  -- Statistics for the last 7 days
  ds7d.total_runs AS total_runs_last_7_days,
  ds7d.passed_runs AS passed_runs_last_7_days,
  ds7d.pass_rate_percent AS pass_rate_last_7_days,

FROM
  -- Base view for overall statistics
  `amy_xlml_poc_prod.dag_run_statistic` AS ds
  
  -- Join with other views to get stats for different timeframes
LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastruns_1` AS ds1r ON ds.dag_id = ds1r.dag_id
LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastruns_3` AS ds3r ON ds.dag_id = ds3r.dag_id
LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastruns_7` AS ds7r ON ds.dag_id = ds7r.dag_id

LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastdays_1` AS ds1d ON ds.dag_id = ds1d.dag_id
LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastdays_3` AS ds3d ON ds.dag_id = ds3d.dag_id
LEFT JOIN
  `amy_xlml_poc_prod.dag_run_statistic_lastdays_7` AS ds7d ON ds.dag_id = ds7d.dag_id

LEFT JOIN
  dag_cleaned_owners AS dco ON ds.dag_id = dco.dag_id
LEFT JOIN
  dag_with_tag AS dt ON ds.dag_id = dt.dag_id
