CREATE OR REPLACE VIEW `amy_xlml_poc_prod.global_dag_summary` AS

-- Calculate global statistics for all time
WITH
  data_time AS (
  SELECT
    FORMAT_DATE('%Y-%m-%d', MIN(updated_at)) upd_start,
    FORMAT_DATE('%Y-%m-%d', MIN(start_date)) data_start,
    FORMAT_DATE('%Y-%m-%d', MAX(end_date)) data_end
  FROM
    `amy_xlml_poc_prod.dag_run` dr
  WHERE
    dr.start_date IS NOT NULL
      AND dr.end_date IS NOT NULL
      AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
      AND dr.dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.ignore_dags`)
  ),
    
  cluster_info AS (
  SELECT
    COUNT(DISTINCT CONCAT(t1.project_name, t1.cluster_name)) AS total_clusters,
    COUNT(DISTINCT t1.project_name) AS total_projects,
    COUNT(DISTINCT t1.dag_id) AS total_cluster_dag,
    COUNT(DISTINCT CONCAT(t1.dag_id, t1.test_id)) AS total_cluster_test,
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t1 ),
  prj_cluster AS (
  SELECT
    ARRAY_AGG(STRUCT(project_name,
        cluster_count,
        cluster_tests)) AS project_cluster_counts
  FROM (
    SELECT
      project_name,
      COUNT(DISTINCT cluster_name) AS cluster_count,
      COUNT(cluster_name) AS cluster_tests,
    FROM
      `amy_xlml_poc_prod.cluster_info_view_latest`
    WHERE
      project_name IS NOT NULL
    GROUP BY
      project_name )),
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
    
  all_time_stats AS (
    SELECT
      COUNT(dag_id) AS total_dags,
      SUM(total_runs) AS total_runs_all_time,
      SUM(passed_runs) AS passed_runs_all_time,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_all_time
    FROM
      `amy_xlml_poc_prod.dag_run_statistic`
  ),

  -- Calculate global stats for the last 1 run
  last_1_run_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_1_run,
      SUM(passed_runs) AS passed_runs_last_1_run,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_1_run
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastruns_1`
  ),

  -- Calculate global stats for the last 3 runs
  last_3_runs_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_3_runs,
      SUM(passed_runs) AS passed_runs_last_3_runs,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_3_runs
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastruns_3`
  ),

  -- Calculate global stats for the last 7 runs
  last_7_runs_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_7_runs,
      SUM(passed_runs) AS passed_runs_last_7_runs,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_7_runs
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastruns_7`
  ),

  -- Calculate global stats for the last 1 day
  last_1_day_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_1_day,
      SUM(passed_runs) AS passed_runs_last_1_day,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_1_day
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastdays_1`
  ),

  -- Calculate global stats for the last 3 days
  last_3_days_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_3_days,
      SUM(passed_runs) AS passed_runs_last_3_days,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_3_days
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastdays_3`
  ),

  -- Calculate global stats for the last 7 days
  last_7_days_stats AS (
    SELECT
      SUM(total_runs) AS total_runs_last_7_days,
      SUM(passed_runs) AS passed_runs_last_7_days,
      ROUND(SAFE_DIVIDE(SUM(passed_runs), SUM(total_runs)) * 100, 2) AS pass_rate_last_7_days
    FROM
      `amy_xlml_poc_prod.dag_run_statistic_lastdays_7`
  ),

  -- First, calculate per-tag aggregations in a separate CTE
  per_tag_aggregations AS (
    SELECT
      tag,
      COUNT(dag_id) AS dags_with_tag,
      SUM(number_of_tests) AS tests_with_tag,
      SUM(total_runs_all_time) AS total_runs_all_time,
      SUM(passed_runs_all_time) AS passed_runs_all_time,
      SUM(total_runs_last_1_run) AS total_runs_last_1_run,
      SUM(passed_runs_last_1_run) AS passed_runs_last_1_run,
      SUM(total_runs_last_3_runs) AS total_runs_last_3_runs,
      SUM(passed_runs_last_3_runs) AS passed_runs_last_3_runs,
      SUM(total_runs_last_7_runs) AS total_runs_last_7_runs,
      SUM(passed_runs_last_7_runs) AS passed_runs_last_7_runs,
      SUM(total_runs_last_1_day) AS total_runs_last_1_day,
      SUM(passed_runs_last_1_day) AS passed_runs_last_1_day,
      SUM(total_runs_last_3_days) AS total_runs_last_3_days,
      SUM(passed_runs_last_3_days) AS passed_runs_last_3_days,
      SUM(total_runs_last_7_days) AS total_runs_last_7_days,
      SUM(passed_runs_last_7_days) AS passed_runs_last_7_days
    FROM
      `amy_xlml_poc_prod.dag_run_summary` AS dr,
      UNNEST(dr.tags) AS tag
    GROUP BY tag
  ),

  -- Then, create the final ARRAY_AGG struct in a new CTE
  per_tag_stats AS (
    SELECT
      ARRAY_AGG(
        STRUCT(
          tag,
          dags_with_tag,
          tests_with_tag,
          total_runs_all_time,
          passed_runs_all_time,
          ROUND(SAFE_DIVIDE(passed_runs_all_time, total_runs_all_time) * 100, 2) AS pass_rate_all_time,
          total_runs_last_1_run,
          passed_runs_last_1_run,
          ROUND(SAFE_DIVIDE(passed_runs_last_1_run, total_runs_last_1_run) * 100, 2) AS pass_rate_last_1_run,
          total_runs_last_3_runs,
          passed_runs_last_3_runs,
          ROUND(SAFE_DIVIDE(passed_runs_last_3_runs, total_runs_last_3_runs) * 100, 2) AS pass_rate_last_3_runs,
          total_runs_last_7_runs,
          passed_runs_last_7_runs,
          ROUND(SAFE_DIVIDE(passed_runs_last_7_runs, total_runs_last_7_runs) * 100, 2) AS pass_rate_last_7_runs,
          total_runs_last_1_day,
          passed_runs_last_1_day,
          ROUND(SAFE_DIVIDE(passed_runs_last_1_day, total_runs_last_1_day) * 100, 2) AS pass_rate_last_1_day,
          total_runs_last_3_days,
          passed_runs_last_3_days,
          ROUND(SAFE_DIVIDE(passed_runs_last_3_days, total_runs_last_3_days) * 100, 2) AS pass_rate_last_3_days,
          total_runs_last_7_days,
          passed_runs_last_7_days,
          ROUND(SAFE_DIVIDE(passed_runs_last_7_days, total_runs_last_7_days) * 100, 2) AS pass_rate_last_7_days
        )
      ) AS tag_summary
    FROM
      per_tag_aggregations
  )

-- Final global summary table
SELECT
  t0.data_start,
  t0.data_end,
  t1.*,
  t2.*,
  t3.project_cluster_counts,
  all_time_stats.*,
  last_1_run_stats.*,
  last_3_runs_stats.*,
  last_7_runs_stats.*,
  last_1_day_stats.*,
  last_3_days_stats.*,
  last_7_days_stats.*,
  per_tag_stats.tag_summary
FROM
  data_time AS t0,
  cluster_info AS t1,
  abnormal_counts AS t2,
  all_time_stats,
  last_1_run_stats,
  last_3_runs_stats,
  last_7_runs_stats,
  last_1_day_stats,
  last_3_days_stats,
  last_7_days_stats,
  per_tag_stats
CROSS JOIN
  prj_cluster AS t3
