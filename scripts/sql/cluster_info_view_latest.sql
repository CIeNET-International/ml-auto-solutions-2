CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_info_view_latest` AS
WITH
gke_deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY cluster_name ORDER BY project_name) AS rn
  FROM
    `amy_xlml_poc_prod.gke_list_clusters` 
),
-- CTE for rows that already have a non-empty project_name
prj_v1 AS (
  SELECT
    dag_id,
    test_id,
    task_id,
    run_id,
    cluster_name,
    region,
    project_name,
    accelerator_type,
    accelerator_family
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest_db`
  WHERE
    project_name IS NOT NULL
    AND project_name != ''
),
-- CTE for rows with an empty project_name, joined with gke_deduped
no_prj_v1 AS (
  SELECT
    t1.dag_id,
    t1.test_id,
    t1.task_id,
    t1.run_id,
    t1.cluster_name,
    t1.region,
    t2.project_name,
    -- Use project_name from the de-duplicated GKE list
    t1.accelerator_type,
    t1.accelerator_family
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest_db` AS t1
  LEFT JOIN
    gke_deduped AS t2
  ON
    t1.cluster_name = t2.cluster_name
  WHERE
    (t1.project_name IS NULL
      OR t1.project_name = '')
    AND t2.rn = 1
),

prj_v2 AS (
  SELECT
    t3.dag_id,
    t3.test_id,
    t3.task_id,
    t3.run_id,
    t3.cluster_name,
    t3.region,
    t3.project_name,
    '' AS accelerator_type,
    '' AS accelerator_family
  FROM
    `amy_xlml_poc_prod.cluster_info_from_log` AS t3
  WHERE
    project_name IS NOT NULL
    AND project_name != '' 
),

no_prj_v2 AS (
  SELECT
    t1.dag_id,
    t1.test_id,
    t1.task_id,
    t1.run_id,
    t1.cluster_name,
    t1.region,
    t2.project_name,
    -- Use project_name from the de-duplicated GKE list
    '' AS accelerator_type,
    '' AS accelerator_family
  FROM
    `amy_xlml_poc_prod.cluster_info_from_log` AS t1
  LEFT JOIN
    gke_deduped AS t2
  ON
    t1.cluster_name = t2.cluster_name
  WHERE
    (t1.project_name IS NULL
      OR t1.project_name = '')
    AND t2.rn = 1
),
union_rows AS (
  SELECT * FROM prj_v1
  UNION ALL
  SELECT * FROM no_prj_v1
  UNION ALL
  SELECT * FROM prj_v2
  UNION ALL 
  SELECT * FROM no_prj_v2
)

SELECT
  r.dag_id,
  b.category, b.accelerator, b.tags, b.dag_owners, b.schedule_interval, b.formatted_schedule, b.is_paused,
  r.test_id,
  r.task_id,
  r.run_id,
  r.cluster_name,
  r.region,
  r.project_name,
  r.accelerator_type,
  r.accelerator_family,
FROM
  union_rows r
JOIN `amy_xlml_poc_prod.base` AS b
ON r.dag_id = b.dag_id


