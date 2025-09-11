CREATE OR REPLACE VIEW `amy_xlml_poc_prod.global_cluster_view` AS
WITH
cluster_status_time AS (
  SELECT min(load_time) AS cluster_check_time
  FROM `amy_xlml_poc_prod.gke_cluster_info`  
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
      v.project_name, v.cluster_name 
    HAVING
      cluster_dags>0
    )
)

SELECT
  t2.abnormal_cluster,
  t2.abnormal_nodepool,
  t4.cluster_check_time,
  t6.cluster_counts abnormal_cluster_counts
FROM
  cluster_status_time AS t4,
  abnormal_counts AS t2
CROSS JOIN
  abnormal_clusters as t6

