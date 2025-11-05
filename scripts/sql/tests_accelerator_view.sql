
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.tests_accelerator_view` AS

WITH
gpu AS (
  select dag_id,test_id,accelerator_type,accelerator_family FROM `cienet-cmcs.amy_xlml_poc_prod.dag_test_info` where accelerator_type is not null AND accelerator_family='GPU'
), 
tpu AS (
  select dag_id,test_id,accelerator_type,accelerator_family FROM `cienet-cmcs.amy_xlml_poc_prod.dag_test_info` where accelerator_type is not null AND accelerator_family='TPU'
),
cpu AS (
  select dag_id,test_id,accelerator_type,accelerator_family FROM `cienet-cmcs.amy_xlml_poc_prod.dag_test_info` where accelerator_type is not null AND accelerator_family='CPU' 
    AND REGEXP_CONTAINS(LOWER(accelerator_type), r'^(n[0-9]+|e[0-9]+|c[0-9]+|m[0-9]+|g[0-9]+|a[0-9]+|f1|h3)')
),

accelerator_from_info AS (
  SELECT * FROM gpu UNION ALL
  SELECT * FROM tpu UNION ALL
  SELECT * FROM cpu
),
flattened_metadata AS (
SELECT
    T_pivot.* -- Select all columns from the pivoted result
  FROM (
    -- **STEP 1: Define the Source Data (JOIN)**
    -- This inner SELECT/JOIN is the input table for the PIVOT.
    SELECT
      t1.job_uuid,
      t2.job_name,
      t1.metadata_key,
      t1.metadata_value
    FROM 
      `cloud-ml-auto-solutions.xlml_dataset.metadata_history` AS t1 -- The EAV table
    INNER JOIN 
      `cloud-ml-auto-solutions.xlml_dataset.job_history` AS t2 
      ON t1.job_uuid = t2.uuid
  ) AS T_source -- Alias for the source data
  
  -- **STEP 2: Apply the PIVOT Operator**
  -- PIVOT must be applied to a table reference (which T_source is)
  PIVOT(
    MAX(metadata_value)  -- Aggregate function applied to metadata_value
    FOR metadata_key IN ('dag_id', 'run_id', 'accelerator') -- The keys to pivot
  ) AS T_pivot -- Alias for the pivoted result
),

latest_n_runs AS (
  SELECT base.dag_id, runs.run_id, runs.execution_date, runs.start_date run_start_date, runs.end_date run_end_date, run_order_desc
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE run_order_desc<=3
),

accelerator_from_last_n AS (
  SELECT r.*,f.job_name AS test_id,
    f.accelerator AS accelerator_type,
    CASE
      WHEN REGEXP_CONTAINS(LOWER(f.accelerator), r'(^ct|tpu|v\d+)') THEN 'TPU'      
      WHEN REGEXP_CONTAINS(LOWER(f.accelerator), r'(nvidia|gpu|a100|h100|t4|v100|k80|l4|l40|l40s|p4|p100|h200)') THEN 'GPU'
      WHEN REGEXP_CONTAINS(LOWER(f.accelerator), r'^(n[0-9]+|e[0-9]+|c[0-9]+|m[0-9]+|g[0-9]+|a[0-9]+|f1|h3)') THEN 'CPU'      
     ELSE 'TBD'
    END AS accelerator_family
  FROM latest_n_runs r 
  LEFT JOIN flattened_metadata f ON r.dag_id=f.dag_id AND r.run_id=f.run_id 
  WHERE f.accelerator IS NOT NULL
),

unique_one AS (
  SELECT *
  FROM accelerator_from_last_n
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY dag_id, test_id
      ORDER BY run_order_desc ASC 
    ) = 1
),

final_prioritized_data AS (
  SELECT
    COALESCE(hp.dag_id, lp.dag_id) AS dag_id,
    COALESCE(hp.test_id, lp.test_id) AS test_id,
    COALESCE(hp.accelerator_type, lp.accelerator_type) AS accelerator_type,
    COALESCE(hp.accelerator_family, lp.accelerator_family) AS accelerator_family    
  FROM
    unique_one hp
  FULL OUTER JOIN 
    accelerator_from_info lp
    ON hp.dag_id = lp.dag_id AND hp.test_id = lp.test_id
)

SELECT * FROM final_prioritized_data

