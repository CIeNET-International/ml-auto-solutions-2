CREATE OR REPLACE VIEW `amy_xlml_poc_prod.global_dag_summary_view` AS

-- Calculate global statistics for all time
WITH
all_dags AS (
  SELECT dag_id, dag_owners, formatted_schedule, is_paused, tags, category, accelerator, 
    total_runs AS total_runs_all, total_runs_iq, passed_runs, partial_passed_runs, failed_runs, quarantined_runs, 
    total_tests, total_tests_qe, total_tests_q, test_ids, test_ids_qe, test_ids_q,
    last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

cnt_qe AS (
  SELECT 
    SUM(total_runs_all) total_runs,
    SUM(total_runs_iq) total_runs_iq,
    SUM(passed_runs) passed_runs,
    SUM(partial_passed_runs) partial_passed_runs,
    SUM(failed_runs) failed_runs,
    SUM(quarantined_runs) quarantined_runs,
    SUM(total_tests) total_tests, 
    SUM(total_tests_qe) total_tests_qe,
    SUM(total_tests_q) total_tests_q,
    COUNT(DISTINCT dag_id) total_dags,
    COUNT(DISTINCT CASE WHEN total_tests_q>0 THEN dag_id END) AS total_dags_q
  FROM all_dags
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

all_dag_count AS (
  SELECT
    -- total distinct dag count from the base table
    (SELECT COUNT(DISTINCT dag_id)
     FROM all_dags) AS total_dags,

    (SELECT COUNT(DISTINCT dag_id)
     FROM all_dags
     WHERE total_runs_all>0) AS total_run_dags_eqd,     

    (SELECT COUNT(DISTINCT dag_id)
     FROM all_dags
     WHERE total_tests=total_tests_q) AS total_run_dags_qd,     

    (SELECT COUNT(DISTINCT dag_id)
     FROM all_dags
     WHERE is_paused=TRUE) AS total_paused,          
    
    -- category counts as ARRAY<STRUCT<category STRING, dag_count INT64>>
    (SELECT ARRAY_AGG(STRUCT(category, category_dags) ORDER BY category)
     FROM (
       SELECT category, COUNT(DISTINCT dag_id) AS category_dags
       FROM all_dags
       GROUP BY category
     )
    ) AS category_counts,

    -- accelerator counts as ARRAY<STRUCT<accelerator STRING, dag_count INT64>>
    (SELECT ARRAY_AGG(STRUCT(accelerator, accelerator_dags) ORDER BY accelerator)
     FROM (
       SELECT accelerator, COUNT(DISTINCT dag_id) AS accelerator_dags
       FROM all_dags
       GROUP BY accelerator
     )
    ) AS accelerator_counts,

    (SELECT ARRAY_AGG(STRUCT(category, total_tests, total_tests_qe, total_tests_q) ORDER BY category)
     FROM (
       SELECT category, SUM(total_tests) AS total_tests, SUM(total_tests_q) AS total_tests_q, SUM(total_tests_qe) AS total_tests_qe
       FROM all_dags
       GROUP BY category
     )
    ) AS category_tests_counts,

    (SELECT ARRAY_AGG(STRUCT(accelerator, total_tests, total_tests_qe, total_tests_q) ORDER BY accelerator)
     FROM (
       SELECT accelerator, SUM(total_tests) AS total_tests, SUM(total_tests_q) AS total_tests_q, SUM(total_tests_qe) AS total_tests_qe
       FROM all_dags
       GROUP BY accelerator
     )
    ) AS accelerator_tests_counts,
),

data_time AS (
  SELECT
    COUNT(DISTINCT dag_id) total_dags,
    --COUNT(DISTINCT CONCAT(dag_id,test_id)) total_tests,
    --FORMAT_DATE('%Y-%m-%d', MIN(updated_at)) upd_start,
    CONCAT(FORMAT_DATE('%Y-%m-%d', MIN(start_date)),' 00:00:00') data_start,
    CONCAT(FORMAT_DATE('%Y-%m-%d', MAX(start_date)),' 23:59:59') data_end
  FROM
    all_tests 
),

dag_sum AS (
  SELECT t.*, c.total_run_dags_eqd, c.total_run_dags_qd, c.total_paused, c.category_counts, c.accelerator_counts, c.category_tests_counts, c.accelerator_tests_counts, q.total_tests, q.total_tests_qe, q.total_tests_q, q.total_runs, q.passed_runs, q.partial_passed_runs, q.total_dags_q
  FROM data_time t, all_dag_count c, cnt_qe q
),

cluster_status_time AS (
  SELECT min(load_time) AS cluster_check_time
  FROM `amy_xlml_poc_prod.gke_cluster_info`  
),

last_one_run_stat AS (
  SELECT * from `amy_xlml_poc_prod.statistic_last_window` where window_name='the_r_1'
),
last_one_run AS (
  SELECT 
    (SELECT SUM(total_run_tests) 
      FROM last_one_run_stat t, UNNEST(t.dag_statistic) s) AS total_run_tests_r1,
    (SELECT SUM(passed_tests) 
      FROM last_one_run_stat t, UNNEST(t.dag_statistic) s) AS passed_tests_r1,
    (SELECT ARRAY_AGG(STRUCT(category, total_run_tests, passed_tests) ORDER BY category)
     FROM (
       SELECT s.category,s.total_run_tests,s.passed_tests from last_one_run_stat t, UNNEST(t.dag_category_statistic) s
     )
    ) AS category_tests_counts_r1,
   (SELECT ARRAY_AGG(STRUCT(accelerator, total_run_tests, passed_tests) ORDER BY accelerator)
     FROM (
       SELECT s.accelerator,s.total_run_tests,s.passed_tests from last_one_run_stat t, UNNEST(t.dag_accelerator_statistic) s
     )
    ) AS accelerator_tests_counts_r1
),
    
cluster_info AS (
  SELECT
    COUNT(DISTINCT CONCAT(t1.project_name, t1.cluster_name, t1.region)) AS total_clusters,
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
        region,
        cluster_dags,
        cluster_tests)) AS cluster_counts
  FROM (
    SELECT
      project_name,
      cluster_name,
      region,
      COUNT(DISTINCT dag_id) AS cluster_dags,
      COUNT(DISTINCT CONCAT(dag_id, test_id)) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest`
    WHERE
      project_name IS NOT NULL AND cluster_name is not null
    GROUP BY
      project_name, cluster_name, region )
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
      region,
      COUNT(1) AS abnormal_nodepool_count
    FROM
      `amy_xlml_poc_prod.gke_cluster_info`,
      UNNEST(node_pools) AS np
    WHERE
      np.status != 'RUNNING'
    GROUP BY
      project_id,
      cluster_name,
      region ) AS t2
  ON
    t1.project_id = t2.project_id
    AND t1.cluster_name = t2.cluster_name
    AND t1.region = t2.region
),

abnormal_clusters AS (
  SELECT
    ARRAY_AGG(STRUCT(project_name,
        cluster_name,
        region,
        cluster_dags,
        cluster_tests)) AS cluster_counts
  FROM (
    SELECT
      v.project_name,
      v.cluster_name,
      v.region,
      COUNT(DISTINCT dag_id) AS cluster_dags,
      COUNT(DISTINCT CONCAT(dag_id, test_id)) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest` v
     JOIN  (
      SELECT project_id,cluster_name,region
      FROM `amy_xlml_poc_prod.gke_cluster_info`
      WHERE status != 'RUNNING' ) AS t1
    ON t1.project_id = v.project_name AND t1.cluster_name = v.cluster_name AND t1.region = v.region
    WHERE
      v.project_name IS NOT NULL AND v.cluster_name IS NOT NULL
    GROUP BY
      v.project_name, v.cluster_name , v.region
    HAVING
      cluster_dags>0
  )
)  

-- Final global summary table
SELECT
  t0.data_start,
  t0.data_end,
  t0.total_dags,
  t0.total_run_dags_eqd + t0.total_run_dags_qd AS total_run_dags,
  t0.total_run_dags_eqd,
  t0.total_run_dags_qd,  
  t0.total_dags_q,
  t0.total_paused,
  t0.category_counts,
  t0.accelerator_counts,
  t0.category_tests_counts,
  t0.accelerator_tests_counts,  
  t1.total_cluster_dag,
  t0.total_runs, 
  t0.passed_runs,
  t0.partial_passed_runs,
  t0.total_tests,
  t0.total_tests_qe,
  t0.total_tests_q,
  t1.total_cluster_test,
  t1.total_clusters,
  t1.total_projects,
  r1.total_run_tests_r1,
  r1.passed_tests_r1,
  r1.category_tests_counts_r1,
  r1.accelerator_tests_counts_r1,
  t2.abnormal_cluster,
  t2.abnormal_nodepool,
  t4.cluster_check_time,
  t3.project_cluster_counts,
  t5.cluster_counts,
  t6.cluster_counts abnormal_cluster_counts
FROM
  dag_sum AS t0,
  cluster_status_time AS t4,
  cluster_info AS t1,
  last_one_run AS r1,
  abnormal_counts AS t2
CROSS JOIN
  prj_cluster AS t3
CROSS JOIN
  clusters AS t5
CROSS JOIN
  abnormal_clusters as t6

