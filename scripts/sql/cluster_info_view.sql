CREATE OR REPLACE VIEW `amy_xlml_poc_2.cluster_info_view` AS
WITH
  cluster_name_cte AS (
    SELECT
      op.dag_id,
      op.run_id,
      op.task_id,
      REGEXP_EXTRACT(op.op_arg_item, r"cluster_name='([^']*)'") AS cluster_name
    FROM
      `amy_xlml_poc_2.op_args_unnested_view` AS op
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
    WHERE
      op_arg_item LIKE "%GCPConfig(project_name=%"
  )

SELECT
  cn.dag_id,
  cn.task_id,
  cn.run_id,
  cn.cluster_name,
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

