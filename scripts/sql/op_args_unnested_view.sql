CREATE OR REPLACE VIEW `amy_xlml_poc_prod.op_args_unnested_view` AS
WITH
  op_args_string AS (
    SELECT
      dag_id,
      task_id,
      run_id,
      rendered_fields,
      JSON_VALUE(SAFE.PARSE_JSON(REPLACE(rendered_fields, '\\\'', '"')), '$.op_args') AS op_args
    FROM
      `amy_xlml_poc_prod.rendered_task_instance_fields`
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

