CREATE OR REPLACE VIEW `amy_xlml_poc_prod.global_dag_summary_view` AS

-- Calculate global statistics for all time
WITH
all_dags AS (
  SELECT dag_id, dag_owners, formatted_schedule, is_paused, total_runs AS total_runs_all, passed_runs, total_tests, tags, category, accelerator, last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

all_runs AS (
  SELECT base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
), 

all_tests AS (
  SELECT base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
),

data_time AS (
  SELECT
    COUNT(DISTINCT dag_id) total_dags,
    COUNT(DISTINCT CONCAT(dag_id,test_id)) total_tests,
    --FORMAT_DATE('%Y-%m-%d', MIN(updated_at)) upd_start,
    FORMAT_DATE('%Y-%m-%d', MIN(start_date)) data_start,
    FORMAT_DATE('%Y-%m-%d', MAX(end_date)) data_end
  FROM
    all_tests
),

cluster_status_time AS (
  SELECT min(load_time) AS cluster_check_time
  FROM `amy_xlml_poc_prod.gke_cluster_info`  
),
    
cluster_info AS (
  SELECT
    COUNT(DISTINCT CONCAT(t1.project_name, t1.cluster_name)) AS total_clusters,
    COUNT(DISTINCT t1.project_name) AS total_projects,
    COUNT(DISTINCT t1.dag_id) AS total_cluster_dag,
    COUNT(DISTINCT CONCAT(t1.dag_id, t1.test_id)) AS total_cluster_test,
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t1 
),

prj_cluster AS (
  SELECT
    ARRAY_AGG(STRUCT(project_name,
        cluster_count,
        cluster_dags,
        cluster_tests)) AS project_cluster_counts
  FROM (
    SELECT
      project_name,
      COUNT(DISTINCT cluster_name) AS cluster_count,
      COUNT(DISTINCT dag_id) AS cluster_dags,
      COUNT(DISTINCT CONCAT(dag_id, test_id)) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest`
    WHERE
      project_name IS NOT NULL
    GROUP BY
      project_name )
),

clusters AS (
  SELECT
    ARRAY_AGG(STRUCT(project_name,
        cluster_name,
        cluster_dags,
        cluster_tests)) AS cluster_counts
  FROM (
    SELECT
      project_name,
      cluster_name,
      COUNT(DISTINCT dag_id) AS cluster_dags,
      COUNT(DISTINCT CONCAT(dag_id, test_id)) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest`
    WHERE
      project_name IS NOT NULL AND cluster_name is not null
    GROUP BY
      project_name, cluster_name )
),

abnormal_counts AS (
  SELECT
    COUNT(CASE
        WHEN t1.status != 'RUNNING' THEN 1
    END
      ) AS abnormal_cluster,
    SUM(abnormal_nodepool_count) AS abnormal_nodepool
  FROM
    `amy_xlml_poc_prod.gke_cluster_info` AS t1
  LEFT JOIN (
    SELECT
      project_id,
      cluster_name,
      COUNT(1) AS abnormal_nodepool_count
    FROM
      `amy_xlml_poc_prod.gke_cluster_info`,
      UNNEST(node_pools) AS np
    WHERE
      np.status != 'RUNNING'
    GROUP BY
      project_id,
      cluster_name ) AS t2
  ON
    t1.project_id = t2.project_id
    AND t1.cluster_name = t2.cluster_name
),
    
abnormal_clusters AS (
  SELECT
    ARRAY_AGG(STRUCT(project_name,
        cluster_name,
        cluster_dags,
        cluster_tests)) AS cluster_counts
  FROM (
    SELECT
      v.project_name,
      v.cluster_name,
      COUNT(DISTINCT dag_id) AS cluster_dags,
      COUNT(DISTINCT CONCAT(dag_id, test_id)) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest` v
     JOIN  (
      SELECT project_id,cluster_name 
      FROM `amy_xlml_poc_prod.gke_cluster_info`
      WHERE status != 'RUNNING' ) AS t1
    ON t1.project_id = v.project_name AND t1.cluster_name = v.cluster_name
    WHERE
      v.project_name IS NOT NULL AND v.cluster_name IS NOT NULL
    GROUP BY
      v.project_name, v.cluster_name HAVING
      COUNT(cluster_name) > 1
      OR COUNT(DISTINCT dag_id) > 1 )
)

-- Final global summary table
SELECT
  t0.data_start,
  t0.data_end,
  t0.total_dags,
  t1.total_cluster_dag,
  t0.total_tests,
  t1.total_cluster_test,
  t1.total_clusters,
  t1.total_projects,
--  t2.abnormal_cluster,
--  t2.abnormal_nodepool,
  t4.cluster_check_time,
  t3.project_cluster_counts,
  t5.cluster_counts,
--  t6.cluster_counts abnormal_cluster_counts
FROM
  data_time AS t0,
  cluster_status_time AS t4,
  cluster_info AS t1
--  abnormal_counts AS t2
CROSS JOIN
  prj_cluster AS t3
CROSS JOIN
  clusters AS t5
--CROSS JOIN
--  abnormal_clusters as t6
