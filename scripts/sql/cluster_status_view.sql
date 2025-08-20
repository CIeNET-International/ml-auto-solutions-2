CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_status_view` AS
WITH dag_with_tag AS (
  SELECT dt.dag_id, ARRAY_AGG(dt.name) AS tags
  FROM `amy_xlml_poc_2.dag_tag` dt
  GROUP BY dt.dag_id
)
SELECT
    civ.dag_id,
    civ.test_name,
    gci.project_id,
    civ.cluster_name,
    civ.region,
    gci.region AS real_region,
    dag_with_tag.tags,
    dti.accelerator_type,
    dti.num_slices,    
    gci.status,
    gci.node_pools,
    gci.load_time,
FROM
    `amy_xlml_poc_2.cluster_info_view_latest` civ
LEFT JOIN
    `amy_xlml_poc_2.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
LEFT JOIN
    dag_with_tag ON civ.dag_id = dag_with_tag.dag_id
LEFT JOIN
    `amy_xlml_poc_2.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_name = dti.test_id;

