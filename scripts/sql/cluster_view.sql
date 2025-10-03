CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_view` AS

WITH 
hacked_clusters AS (
  SELECT "cluster-1" AS cluster_name, "supercomputer-testing" AS project_name
),

clusters AS (
SELECT DISTINCT
  --cluster_project as project_name,
  project_name,
  cluster_name
FROM
   `amy_xlml_poc_prod.cluster_info_view_latest`
--  `amy_xlml_poc_prod.test_cluster_mapping`
WHERE
  --cluster_project IS NOT NULL AND 
  project_name IS NOT NULL AND 
  cluster_name IS NOT NULL
)


SELECT project_name,cluster_name FROM clusters
UNION ALL
SELECT project_name,cluster_name FROM hacked_clusters

