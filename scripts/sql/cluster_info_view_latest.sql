CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_info_view_latest` AS
-- Select from the first source table
SELECT
  t1.dag_id,
  t1.test_id,
  t1.task_id,
  t1.run_id,
  t1.cluster_name,
  t1.region,
  -- Use COALESCE to get project_name from gke_list_clusters if source is NULL or empty
  COALESCE(NULLIF(t1.project_name, ''), t2.project_name) AS project_name,
  t1.accelerator_type  
FROM
  `amy_xlml_poc_prod.cluster_info_view_latest_db` AS t1
LEFT JOIN
  `amy_xlml_poc_prod.gke_list_clusters` AS t2
  ON t1.cluster_name = t2.cluster_name

UNION ALL

-- Select from the second source table
SELECT
  t3.dag_id,
  t3.test_id,
  t3.task_id,
  t3.run_id,
  t3.cluster_name,
  t3.region,
  -- Use COALESCE to get project_name from gke_list_clusters if source is NULL or empty
  COALESCE(NULLIF(t3.project_name, ''), t4.project_name) AS project_name,
  '' AS accelerator_type  
FROM
  `amy_xlml_poc_prod.cluster_info_from_log` AS t3
LEFT JOIN
  `amy_xlml_poc_prod.gke_list_clusters` AS t4
  ON t3.cluster_name = t4.cluster_name

