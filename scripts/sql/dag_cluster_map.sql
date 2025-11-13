CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_cluster_map` AS
WITH grouping_tests AS (
  -- Step 1: Group by cluster definition and collect all test IDs run on it
  SELECT
    t.dag_id,
    t.cluster_name,
    t.project_name,
    t.region,
    t.accelerator_type,
    t.accelerator_family,
    ANY_VALUE(t.is_quarantined_dag) AS is_quarantined_dag,    
    ANY_VALUE(t.category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags,
    ANY_VALUE(t.dag_owners) AS dag_owners, ANY_VALUE(t.schedule_interval) AS schedule_interval, 
    ANY_VALUE(t.formatted_schedule) AS formatted_schedule, ANY_VALUE(t.is_paused) AS is_paused,
    ARRAY_AGG(
      STRUCT(
        t.test_id AS test_id,
        t.is_quarantined_test AS is_quarantined_test
      )
      IGNORE NULLS 
    ) AS tests
  FROM
    `amy_xlml_poc_prod.cluster_info_view_latest` AS t
  GROUP BY 1, 2, 3, 4, 5, 6
)
-- Step 2: Aggregate the cluster structs under each DAG
SELECT
  t.dag_id,
    ANY_VALUE(t.category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags,
    ANY_VALUE(t.dag_owners) AS dag_owners, ANY_VALUE(t.schedule_interval) AS schedule_interval, 
    ANY_VALUE(t.formatted_schedule) AS formatted_schedule, ANY_VALUE(t.is_paused) AS is_paused,
    ANY_VALUE(t.is_quarantined_dag) AS is_quarantined_dag,
  ARRAY_AGG(
    STRUCT(
      t.cluster_name,
      t.project_name,
      t.region,
      t.accelerator_type,
      t.accelerator_family,
      t.tests
    ) 
    IGNORE NULLS 
    ORDER BY t.cluster_name
  ) AS clusters
FROM
  grouping_tests AS t
GROUP BY 1


