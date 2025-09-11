
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_run_statistic` AS

WITH
all_dags AS (
  SELECT dag_id, dag_owners, formatted_schedule, is_paused, total_runs, passed_runs, total_tests, tags
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

all_runs AS (
  SELECT base.dag_id, base.total_runs, base.total_tests, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
), 

last_run AS (
  SELECT base.dag_id, base.total_runs, base.total_tests, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE runs.run_order_desc=1
), 

last_succ AS (
  SELECT base.dag_id, base.total_runs, base.total_tests, runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE is_passed=1 AND runs.is_passed_run_order_desc=1
), 

-- DAG run statistic 
dag_statistic AS (    
  SELECT 
    dag_id,
    total_runs,
    passed_runs,
    ROUND(SAFE_DIVIDE(passed_runs, total_runs) * 100, 2) AS pass_rate_percent,
    total_tests AS total_tests
  FROM all_dags
),


dag_clusters AS (
  SELECT
    t1.dag_id,
    ARRAY_AGG(STRUCT(t1.project_name, t1.cluster_name, t2.machine_families)) AS clusters
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t1
  JOIN
    `amy_xlml_poc_prod.gke_cluster_info_view` AS t2
  ON
    t1.project_name = t2.project_id
    AND t1.cluster_name = t2.cluster_name
  GROUP BY
    t1.dag_id
)


-- Final result
SELECT d.dag_id, d.total_runs, d.passed_runs, ds.pass_rate_percent, d.total_tests AS number_of_tests,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',ls.start_date)  AS last_succ, 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',lr.start_date) AS last_exec, 
  d.is_paused,d.formatted_schedule,
  dc.clusters,
  d.tags, d.total_runs AS total_runs_all,d.dag_owners AS dag_owner
FROM all_dags d
LEFT JOIN dag_statistic ds ON d.dag_id = ds.dag_id
LEFT JOIN last_succ ls ON ds.dag_id = ls.dag_id
LEFT JOIN last_run lr ON ds.dag_id = lr.dag_id
LEFT JOIN dag_clusters dc ON ds.dag_id = dc.dag_id
ORDER BY ds.dag_id


