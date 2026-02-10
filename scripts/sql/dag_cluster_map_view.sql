CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_cluster_map_view` AS

WITH 
filtered_runs_n AS (
  SELECT w.dag_id, w.tags, w.total_runs, w.total_tests,
    w.run_id, w.execution_date, w.start_date, w.end_date, w.is_passed, w.is_partial_passed
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_runs) w
  WHERE window_name='the_r_1'
),
filtered_runs_qr AS (
  SELECT w.dag_id, w.tags, w.total_runs, w.total_tests,
    w.run_id, w.execution_date, w.start_date, w.end_date, w.is_passed, w.is_partial_passed
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_runs_qr) w
  WHERE window_name='the_r_1'
),
filtered_runs AS (
  SELECT * FROM filtered_runs_n
  UNION ALL
  SELECT * FROM filtered_runs_qr
),
all_dags AS (
  SELECT *,
    CASE WHEN total_tests > 0 AND total_tests != total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN total_tests > 0 AND total_tests = total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag
  FROM `amy_xlml_poc_prod.base`
),

filtered_tests AS (
  SELECT w.dag_id, w.test_id, w.test_is_passed, w.test_start_date, w.test_end_date, w.test_accelerator, w.type,
    c.cluster_name, c.project_name, c.region
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name='the_r_1'
),

filtered_tests_qr_qt AS (
  SELECT w.dag_id, w.test_id, w.test_is_passed, w.test_start_date, w.test_end_date, w.test_accelerator, w.type,
    c.cluster_name, c.project_name, c.region
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests_qr_qt) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name='the_r_1'
),

last_run_stats_eq AS (
  SELECT
    dag_id,
    COUNT(test_id) AS total_tests_eq,
    SUM(test_is_passed) AS total_passed_eq
   FROM
    filtered_tests AS t1
  GROUP BY 1
),
last_run_stats_q AS (
  SELECT
    dag_id,
    COUNT(test_id) AS total_tests_q,
    SUM(test_is_passed) AS total_passed_q
   FROM
    filtered_tests_qr_qt AS t1
  GROUP BY 1
),
last_run_cluster_stats_eq AS (
  SELECT
    dag_id,
    cluster_name, project_name, region,
    COUNT(test_id) AS total_tests_eq,
    SUM(test_is_passed) AS total_passed_eq
   FROM
    filtered_tests AS t1
  GROUP BY 1,2,3,4
),
last_run_cluster_stats_q AS (
  SELECT
    dag_id,
    cluster_name, project_name, region,
    COUNT(test_id) AS total_tests_q,
    SUM(test_is_passed) AS total_passed_q
   FROM
    filtered_tests_qr_qt AS t1
  GROUP BY 1,2,3,4
),
last_run_dag_summary AS (
  SELECT
    r.dag_id, r.run_id, r.execution_date, r.start_date, r.end_date, r.is_passed, r.is_partial_passed,
    COALESCE(eq.total_tests_eq, 0) AS total_tests_eq,
    COALESCE(eq.total_passed_eq, 0) AS total_passed_eq,
    COALESCE(q.total_tests_q, 0) AS total_tests_q,
    COALESCE(q.total_passed_q, 0) AS total_passed_q
  FROM filtered_runs AS r
  FULL OUTER JOIN last_run_stats_eq AS eq ON r.dag_id = eq.dag_id
  FULL OUTER JOIN last_run_stats_q AS q ON r.dag_id = q.dag_id
),

last_run_cluster_agg AS (
  SELECT
    COALESCE(eq.dag_id, q.dag_id) AS dag_id,
    ARRAY_AGG(
      STRUCT(
        COALESCE(eq.cluster_name, q.cluster_name) AS cluster_name,
        COALESCE(eq.project_name, q.project_name) AS project_name,
        COALESCE(eq.region, q.region) AS region,
        COALESCE(eq.total_tests_eq, 0) + COALESCE(q.total_tests_q, 0) AS total_tests,
        COALESCE(eq.total_tests_eq, 0) AS total_tests_eq,
        COALESCE(eq.total_passed_eq, 0) AS total_passed_eq,
        COALESCE(q.total_tests_q, 0) AS total_tests_q,
        COALESCE(q.total_passed_q, 0) AS total_passed_q
      )
      IGNORE NULLS 
    ) AS last_run_clusters
  FROM last_run_cluster_stats_eq AS eq
  FULL OUTER JOIN last_run_cluster_stats_q AS q 
    ON eq.dag_id = q.dag_id
    AND eq.cluster_name = q.cluster_name
    AND eq.project_name = q.project_name
    AND eq.region = q.region
  GROUP BY 1  
),

grouping_tests AS (
  -- Step 1: Group by cluster definition and collect all test IDs run on it
  SELECT
    t.dag_id,
    t.cluster_name,
    t.project_name,
    t.region,
    t.accelerator_type,
    t.accelerator_family,
    ANY_VALUE(t.is_quarantined_dag) AS is_quarantined_dag,    
    ANY_VALUE(t.category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags,
    ANY_VALUE(t.dag_owners) AS dag_owners, ANY_VALUE(t.schedule_interval) AS schedule_interval, 
    ANY_VALUE(t.formatted_schedule) AS formatted_schedule, ANY_VALUE(t.is_paused) AS is_paused,
    ARRAY_AGG(
      STRUCT(
        t.test_id AS test_id,
        t.is_quarantined_test AS is_quarantined_test
      )
      IGNORE NULLS 
    ) AS tests
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t
  GROUP BY 1, 2, 3, 4, 5, 6
),
grouping_cluster_tests AS (
  SELECT t.dag_id,
    ANY_VALUE(t.category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags,
    ANY_VALUE(t.dag_owners) AS dag_owners, ANY_VALUE(t.schedule_interval) AS schedule_interval, 
    ANY_VALUE(t.formatted_schedule) AS formatted_schedule, ANY_VALUE(t.is_paused) AS is_paused,
    ANY_VALUE(t.is_quarantined_dag) AS is_quarantined_dag,
    ARRAY_AGG(
      STRUCT(
        t.cluster_name,
        t.project_name,
        t.region,
        t.accelerator_type,
        t.accelerator_family,
        t.tests
      ) 
      IGNORE NULLS 
      ORDER BY t.cluster_name
    ) AS clusters,
  FROM grouping_tests AS t
  GROUP BY 1
),
dags_with_clusters AS (
  SELECT
    t.dag_id, t.category, t.accelerator, t.tags, t.dag_owners, t.formatted_schedule, t.is_paused, t.is_quarantined_dag, t.clusters,
    s.run_id, s.execution_date, s.is_passed, s.is_partial_passed, 
    s.total_tests_eq + s.total_tests_q AS total_run_tests, s.total_tests_eq, s.total_tests_q, s.total_passed_eq, s.total_passed_q,
    ca.last_run_clusters
  FROM
    grouping_cluster_tests AS t
  LEFT JOIN
    last_run_dag_summary AS s ON t.dag_id = s.dag_id
  LEFT JOIN
    last_run_cluster_agg AS ca ON t.dag_id = ca.dag_id  
),
dags_wo_clusters AS (
  SELECT
    t.dag_id, t.category, t.accelerator, t.tags, t.dag_owners, t.formatted_schedule, t.is_paused, t.is_quarantined_dag, 
    (SELECT clusters FROM grouping_cluster_tests LIMIT 0) AS clusters,
    s.run_id, s.execution_date, s.is_passed, s.is_partial_passed, 
    s.total_tests_eq + s.total_tests_q AS total_run_tests, s.total_tests_eq, s.total_tests_q, s.total_passed_eq, s.total_passed_q,
    (SELECT last_run_clusters FROM last_run_cluster_agg LIMIT 0) AS last_run_clusters
  FROM all_dags t
  LEFT JOIN
    last_run_dag_summary AS s ON t.dag_id = s.dag_id
  WHERE t.dag_id NOT IN (SELECT dag_id FROM dags_with_clusters)  

)

SELECT * FROM dags_with_clusters 
UNION ALL
SELECT * FROM dags_wo_clusters

