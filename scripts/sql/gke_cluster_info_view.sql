CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.gke_cluster_info_view` AS
WITH 
aggr_category AS (
  SELECT 
    t2.project_name,
    t2.cluster_name,
    t2.region,
    t2.category,
    COUNT(DISTINCT t2.dag_id) AS num_runs,
    COUNT(DISTINCT(CONCAT(t2.dag_id,t2.test_id))) AS num_tests,
  FROM
      `cienet-cmcs.amy_xlml_poc_prod.cluster_info_view_latest` AS t2
  GROUP BY
    t2.project_name,
    t2.cluster_name,
    t2.region,
    t2.category  
),

aggr_accelerator AS (
  SELECT 
    t2.project_name,
    t2.cluster_name,
    t2.region,
    t2.accelerator,
    COUNT(DISTINCT t2.dag_id) AS num_runs,
    COUNT(DISTINCT(CONCAT(t2.dag_id,t2.test_id))) AS num_tests,
  FROM
      `cienet-cmcs.amy_xlml_poc_prod.cluster_info_view_latest` AS t2
  GROUP BY
    t2.project_name,
    t2.cluster_name,
    t2.region,
    t2.accelerator  
),
aggr_dag_test AS (
  SELECT
    t2.project_name,
    t2.cluster_name,
    t2.region,
    COUNT(DISTINCT t2.dag_id) AS num_runs,
    COUNT(DISTINCT(CONCAT(t2.dag_id,t2.test_id))) AS num_tests,
    ARRAY_AGG(
      STRUCT(
        t2.dag_id,
        t2.category,
        t2.accelerator,
        t2.run_id,
        t2.test_id
      )
    ) AS tests_in_use
    FROM
      `cienet-cmcs.amy_xlml_poc_prod.cluster_info_view_latest` AS t2
    GROUP BY
      t2.project_name,
      t2.cluster_name,
      t2.region
),
aggr_dag AS (
  SELECT
    t2.project_name,
    t2.cluster_name,
    t2.region,
    COUNT(DISTINCT t2.dag_id) AS num_runs,
    ARRAY_AGG(
      STRUCT(
        t2.dag_id,
        t2.category,
        t2.accelerator
      )
    ) AS dags_in_use
    FROM
      `cienet-cmcs.amy_xlml_poc_prod.cluster_info_view_latest` AS t2
    GROUP BY
      t2.project_name,
      t2.cluster_name,
      t2.region
),
aggr_1 AS (
  SELECT d.project_name, d.cluster_name, d.region, ANY_VALUE(d.num_runs) num_runs, ANY_VALUE(d.num_tests) num_tests, ANY_VALUE(d.tests_in_use) tests_in_use, 
    ARRAY_AGG(
      STRUCT(
        c.category, c.num_runs, c.num_tests
      )
    ) AS tests_by_category,
    ARRAY_AGG(
      STRUCT(
        a.accelerator, a.num_runs, a.num_tests
      )
    ) AS tests_by_accelerator,
  FROM aggr_dag_test d
  LEFT JOIN aggr_category c ON d.project_name = c.project_name AND d.cluster_name = c.cluster_name AND d.region = c.region
  LEFT JOIN aggr_accelerator a ON d.project_name = a.project_name AND d.cluster_name = a.cluster_name AND d.region = a.region
  GROUP BY d.project_name, d.cluster_name, d.region
),
aggr_2 AS (
  SELECT d.project_name, d.cluster_name, d.region, ANY_VALUE(d.num_runs) num_runs, ANY_VALUE(d.dags_in_use) dags_in_use, 
    ARRAY_AGG(
      STRUCT(
        c.category, c.num_runs
      )
    ) AS dags_by_category,
    ARRAY_AGG(
      STRUCT(
        a.accelerator, a.num_runs
      )
    ) AS dags_by_accelerator,
  FROM aggr_dag d
  LEFT JOIN aggr_category c ON d.project_name = c.project_name AND d.cluster_name = c.cluster_name AND d.region = c.region
  LEFT JOIN aggr_accelerator a ON d.project_name = a.project_name AND d.cluster_name = a.cluster_name AND d.region = a.region
  GROUP BY d.project_name, d.cluster_name, d.region
),

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
    t1.cluster_name,
    t1.project_id,
    t1.region,
    t1.cluster_mode,
    t2.machine_families,
    t1.status,
    t1.status_message,
    t1.load_time,
    t1.node_pools,
    t3.num_runs, t3.num_tests, t3.tests_in_use, t3.tests_by_category, t3.tests_by_accelerator,
    t4.dags_in_use, t4.dags_by_category, t4.dags_by_accelerator
FROM
    `cienet-cmcs.amy_xlml_poc_prod.gke_cluster_info` AS t1
LEFT JOIN cluster_machine_families AS t2 ON
    t1.project_id = t2.project_id
    AND t1.cluster_name = t2.cluster_name 
    AND t1.region = t2.region
LEFT JOIN aggr_1 AS t3 ON
    t1.project_id = t3.project_name
    AND t1.cluster_name = t3.cluster_name
    AND t1.region = t3.region
LEFT JOIN aggr_2 AS t4 ON
    t1.project_id = t4.project_name
    AND t1.cluster_name = t4.cluster_name
    AND t1.region = t4.region        

