CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_status_unnest_view` AS
WITH 
cluster_machine_families AS (
  SELECT
    project_id,
    cluster_name,
    ARRAY_AGG(DISTINCT np.machine_family IGNORE NULLS) AS machine_families
  FROM
    `amy_xlml_poc_prod.gke_cluster_info`,
    UNNEST(node_pools) AS np
  GROUP BY
    project_id,
    cluster_name
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
    cmf.machine_families,
    node_pool.name nodepool_name,
    node_pool.status nodepool_status,
    node_pool.version nodepool_version,
    node_pool.autoscaling_enabled nodepool_autoscaling_enabled,
    node_pool.initial_node_count nodepool_initial_node_count,
    node_pool.node_count nodepool_node_count,
    node_pool.machine_type nodepool_machine_type,
    node_pool.machine_family nodepool_machine_family,
    node_pool.disk_size_gb nodepool_disk_size_gb,
    node_pool.preemptible nodepool_preemptible,
    gci.load_time,
FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` civ
LEFT JOIN
    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
LEFT JOIN
    `amy_xlml_poc_prod.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_id = dti.test_id
LEFT JOIN
    cluster_machine_families cmf ON gci.project_id = cmf.project_id AND gci.cluster_name = cmf.cluster_name
LEFT JOIN UNNEST(gci.node_pools) AS node_pool


