CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_status_view` AS
WITH 
cluster_machine_families AS (
  SELECT
    project_id,
    cluster_name,
    region,
    ARRAY_AGG(DISTINCT np.machine_family IGNORE NULLS) AS machine_families
  FROM
    `amy_xlml_poc_prod.gke_cluster_info`,
    UNNEST(node_pools) AS np
  GROUP BY
    project_id,
    cluster_name,
    region
)
SELECT
    civ.dag_id,
    civ.category,
    civ.accelerator,
    civ.tags,
    civ.dag_owners,
    civ.schedule_interval,
    civ.formatted_schedule,
    civ.is_paused,    
    civ.test_id,
    gci.project_id,
    civ.cluster_name,
    civ.region,
    gci.region AS real_region,
    dti.accelerator_type,
    dti.num_slices,    
    gci.status,
    gci.load_time,
    cmf.machine_families,
    gci.node_pools
FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` civ
LEFT JOIN
--    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = gci.project_id AND civ.cluster_name = gci.cluster_name AND civ.region = gci.region
LEFT JOIN
    `amy_xlml_poc_prod.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_id = dti.test_id
LEFT JOIN
    cluster_machine_families cmf ON gci.project_id = cmf.project_id AND gci.cluster_name = cmf.cluster_name AND gci.region=cmf.region
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_status_view` AS
WITH
cluster_machine_families AS (
  SELECT
    project_id,
    cluster_name,
    region,
    ARRAY_AGG(DISTINCT np.machine_family IGNORE NULLS) AS machine_families
  FROM
    `amy_xlml_poc_prod.gke_cluster_info`,
    UNNEST(node_pools) AS np
  GROUP BY
    project_id,
    cluster_name,
    region
)
SELECT
    civ.dag_id,
    civ.category,
    civ.accelerator,
    civ.tags,
    civ.dag_owners,
    civ.schedule_interval,
    civ.formatted_schedule,
    civ.is_paused,
    civ.test_id,
    gci.project_id,
    civ.cluster_name,
    civ.region,
    gci.region AS real_region,
    dti.accelerator_type,
    dti.num_slices,
    gci.status,
    gci.load_time,
    cmf.machine_families,
    gci.node_pools
FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` civ
LEFT JOIN
--    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = gci.project_id AND civ.cluster_name = gci.cluster_name AND civ.region = gci.region
LEFT JOIN
    `amy_xlml_poc_prod.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_id = dti.test_id
LEFT JOIN
    cluster_machine_families cmf ON gci.project_id = cmf.project_id AND gci.cluster_name = cmf.cluster_name AND gci.region=cmf.region
WHERE civ.is_quarantined_test = FALSE


