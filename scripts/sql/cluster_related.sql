CREATE OR REPLACE VIEW `amy_xlml_poc_2.op_args_unnested_view` AS
WITH
  op_args_string AS (
    SELECT
      dag_id,
      task_id,
      run_id,
      rendered_fields,
      JSON_VALUE(SAFE.PARSE_JSON(REPLACE(rendered_fields, '\\\'', '"')), '$.op_args') AS op_args
    FROM
      `amy_xlml_poc_2.rendered_task_instance_fields`
    WHERE
      rendered_fields like '%cluster_name%'      
  ),
  op_args_array AS (
    SELECT
      dag_id,
      task_id,
      run_id,
      rendered_fields,
      op_args,
      -- Split the string using ',' as a delimiter
      SPLIT(op_args, ',') AS op_args_list
    FROM
      op_args_string
  )
SELECT
  dag_id,
  run_id,
  task_id,
  op_arg_item
FROM
  op_args_array,
  UNNEST(op_args_list) AS op_arg_item;


CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_info_view` AS
WITH
  cluster_name_cte AS (
    SELECT
      dag_id,
      run_id,
      task_id,
      REGEXP_EXTRACT(op_arg_item, r"cluster_name='([^']*)'") AS cluster_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view`
    WHERE
      op_arg_item LIKE "%cluster_name=%"
  ),
  test_name_cte AS (
    SELECT
      dag_id,
      run_id,
      task_id,
      REGEXP_EXTRACT(op_arg_item, r"test_name='([^']*)'") AS test_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view`
    WHERE
      op_arg_item LIKE "%test_name=%"
  ),
  region_cte AS (
    SELECT
      dag_id,
      run_id,
      task_id,
      REGEXP_EXTRACT(op_arg_item, r"zone='([a-zA-Z]+-[a-zA-Z0-9]+)") AS region
    FROM
      `amy_xlml_poc_2.op_args_unnested_view`
    WHERE
      op_arg_item LIKE "%zone=%"
  ),
  project_name_cte AS (
    SELECT
      dag_id,
      run_id,
      task_id,
      REGEXP_EXTRACT(op_arg_item, r"GCPConfig\(project_name='([^']*)'") AS project_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view`
     WHERE
      op_arg_item LIKE "%GCPConfig(project_name=%"
  )
SELECT
  cn.dag_id,
  cn.task_id,
  cn.run_id,
  cn.cluster_name,
  tn.test_name, 
  zn.region,  
  pn.project_name
FROM
  cluster_name_cte AS cn
LEFT JOIN
  test_name_cte AS tn
  ON cn.dag_id = tn.dag_id AND cn.task_id = tn.task_id AND cn.run_id = tn.run_id
LEFT JOIN
  region_cte AS zn
  ON cn.dag_id = zn.dag_id AND cn.task_id = zn.task_id AND cn.run_id = zn.run_id
LEFT JOIN
  project_name_cte AS pn
  ON cn.dag_id = pn.dag_id AND cn.task_id = pn.task_id AND cn.run_id = pn.run_id;


----------------------
CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_info_view_latest` AS
WITH
  latest_dag_run_cte AS (
    SELECT
      dag_id,
      run_id
    FROM
      (
        SELECT
          dag_id,
          run_id,
          ROW_NUMBER() OVER (PARTITION BY dag_id ORDER BY execution_date DESC) AS rn
        FROM
          `amy_xlml_poc_2.dag_run`
        WHERE
          execution_date >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
      ) AS subquery
    WHERE subquery.rn = 1
  ),
  cluster_name_cte AS (
    SELECT
      op.dag_id,
      op.run_id,
      op.task_id,
      REGEXP_EXTRACT(op.op_arg_item, r"cluster_name='([^']*)'") AS cluster_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view` AS op
    INNER JOIN latest_dag_run_cte AS ldr
      ON op.dag_id = ldr.dag_id AND op.run_id = ldr.run_id
    WHERE
      op.op_arg_item LIKE "%cluster_name=%"
  ),
  region_cte AS (
    SELECT
      op.dag_id,
      op.run_id,
      op.task_id,
      REGEXP_EXTRACT(op.op_arg_item, r"zone='([a-zA-Z]+-[a-zA-Z0-9]+)") AS region
    FROM
      `amy_xlml_poc_2.op_args_unnested_view` AS op
    INNER JOIN latest_dag_run_cte AS ldr
      ON op.dag_id = ldr.dag_id AND op.run_id = ldr.run_id
    WHERE
      op_arg_item LIKE "%zone=%"
  ),
  project_name_cte AS (
    SELECT
      op.dag_id,
      op.run_id,
      op.task_id,
      REGEXP_EXTRACT(op_arg_item, r"GCPConfig\(project_name='([^']*)'") AS project_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view` AS op
    INNER JOIN latest_dag_run_cte AS ldr
      ON op.dag_id = ldr.dag_id AND op.run_id = ldr.run_id
    WHERE
      op_arg_item LIKE "%GCPConfig(project_name=%"
  )
SELECT
  cn.dag_id,
  cn.task_id,
  cn.run_id,
  cn.cluster_name,
  REGEXP_EXTRACT(cn.task_id, r"([^.]+)") AS test_name,
  zn.region,      
  pn.project_name
FROM
  cluster_name_cte AS cn
LEFT JOIN
  region_cte AS zn
  ON cn.dag_id = zn.dag_id AND cn.task_id = zn.task_id AND cn.run_id = zn.run_id
LEFT JOIN
  project_name_cte AS pn
  ON cn.dag_id = pn.dag_id AND cn.task_id = pn.task_id AND cn.run_id = pn.run_id;


    
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

---------unnest nodepool-----------------------    
CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_status_unnest_view` AS
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
    node_pool.name nodepool_name,
    node_pool.status nodepool_status,
    node_pool.version nodepool_version,
    node_pool.autoscaling_enabled nodepool_autoscaling_enabled,
    node_pool.initial_node_count nodepool_initial_node_count,
    node_pool.machine_type nodepool_machine_type,
    node_pool.disk_size_gb nodepool_disk_size_gb,
    node_pool.preemptible nodepool_preemptible,
    gci.load_time,
FROM
    `amy_xlml_poc_2.cluster_info_view_latest` civ
LEFT JOIN
    `amy_xlml_poc_2.gke_cluster_info` gci ON civ.project_name = IFNULL(gci.project_id, civ.project_name) AND civ.cluster_name = IFNULL(gci.cluster_name, civ.cluster_name)
LEFT JOIN
    dag_with_tag ON civ.dag_id = dag_with_tag.dag_id
LEFT JOIN
    `amy_xlml_poc_2.dag_test_info` dti ON civ.dag_id = dti.dag_id AND civ.test_name = dti.test_id
LEFT JOIN UNNEST(gci.node_pools) AS node_pool;    
    
---------------------------------------

SELECT
  civ.*
FROM
  `amy_xlml_poc_2.cluster_info_view` AS civ
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      `amy_xlml_poc_2.cluster_info_view_latest` AS civl
    WHERE
      civ.dag_id = civl.dag_id AND civ.task_id = civl.task_id
  );


    

    
SELECT
  dag_id,
  ARRAY_AGG(DISTINCT cluster_value) AS cluster_names_list
FROM
  `amy_xlml_poc_2.cluster_name_view`
WHERE cluster_value <> ''
GROUP BY
  dag_id;

