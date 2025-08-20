CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_view` AS
SELECT DISTINCT
  project_name,
  cluster_name,
  region
FROM
  `amy_xlml_poc_2.cluster_info_view_latest`
WHERE
  project_name IS NOT NULL
  AND cluster_name IS NOT NULL
  AND region IS NOT NULL;
