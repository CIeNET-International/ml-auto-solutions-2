CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.cluster_dag_map` AS
WITH grouping_tests AS (
  -- STEP 1: Group by DAG and Cluster, and aggregate the unique test details (inner aggregation)
  SELECT
    t.cluster_name,
    t.project_name,
    t.region,
    t.dag_id,
    ANY_VALUE(t.is_quarantined_dag) AS is_quarantined_dag,    
    ANY_VALUE(t.category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags,
    ANY_VALUE(t.dag_owners) AS dag_owners, ANY_VALUE(t.schedule_interval) AS schedule_interval, 
    ANY_VALUE(t.formatted_schedule) AS formatted_schedule, ANY_VALUE(t.is_paused) AS is_paused, 
    ARRAY_AGG(
      STRUCT(
        t.test_id AS test_id,
        t.is_quarantined_test AS is_quarantined_test,
        t.accelerator_type,
        t.accelerator_family        
      )
      IGNORE NULLS 
    ) AS tests
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t
  GROUP BY 1, 2, 3, 4 -- Group by Cluster fields and dag_id
)
-- STEP 2: Aggregate the DAG structs under each unique Cluster definition (outer aggregation)
SELECT
  t.cluster_name,
  t.project_name,
  t.region,  
  -- Aggregate all unique DAG structs run on this cluster configuration
  ARRAY_AGG(
    STRUCT(
      t.dag_id,
      t.is_quarantined_dag,t.category, t.accelerator, t.tags,
      t.dag_owners, t.schedule_interval, t.formatted_schedule, t.is_paused,        
      t.tests 
    )
    IGNORE NULLS
    ORDER BY t.dag_id
  ) AS dags
FROM
  grouping_tests AS t
GROUP BY
  t.cluster_name,
  t.project_name,
  t.region

