CREATE OR REPLACE VIEW `amy_xlml_poc_prod.cluster_status_unnest_view` AS
WITH dag_with_tag AS (
  SELECT dt.dag_id, ARRAY_AGG(dt.name) AS tags
  FROM `amy_xlml_poc_prod.dag_tag` dt
  GROUP BY dt.dag_id
)
SELECT
    civ.dag_id,
    civ.test_id,
    gci.project_id,
    civ.cluster_name,
    civ.region,
    gci.region AS real_region,
    dag_with_tag.tags,
    dti.accelerator_type,
    dti.num_slices,    
    gci.status,
    node_pool.name nodepool_name,
    node_pool.status nodepool_status,
    node_pool.version nodepool_version,
    node_pool.autoscaling_enabled nodepool_autoscaling_enabled,
    node_pool.initial_node_count nodepool_initial_node_count,
    node_pool.node_count nodepool_node_count,
    node_pool.machine_type nodepool_machine_type,
    node_pool.disk_size_gb nodepool_disk_size_gb,
    node_pool.preemptible nodepool_preemptible,
    gci.load_time,
FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` civ
LEFT JOIN
    `amy_xlml_poc_prod.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
LEFT JOIN
    dag_with_tag ON civ.dag_id = dag_with_tag.dag_id
LEFT JOIN
    `amy_xlml_poc_prod.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_id = dti.test_id
LEFT JOIN UNNEST(gci.node_pools) AS node_pool;    


