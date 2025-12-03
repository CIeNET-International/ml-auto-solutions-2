CREATE OR REPLACE VIEW
  `cienet-cmcs.amy_xlml_poc_prod.dag_runs_with_logs_view` AS
WITH
  -- 1. base_view_data CTE: Flattens the tests array and calculates the logs_string.
  base_view_data AS (
    SELECT
      dr.dag_id,
      dr.run_id,
      dr.execution_date,
      dr.run_start_date,
      dr.run_end_date,
      dr.run_status,
      t.test_id,
      t.test_start_date,
      t.test_end_date,
      t.test_status,
      t.workload_id,
      t.cluster_project,
      t.cluster_name,
      t.cluster_region,
      t.accelerator_type,
      t.accelerator_family,
      t.machine_families,
      t.airflow_errors, -- Keep raw arrays for error code lookup
      t.airflow_keywords,
      t.k8s_messages,
      t.log_url_error,
      t.log_url_all,
      t.log_url_k8s,
      t.log_url_graph,
      -- Logs String format preserved
      ARRAY_TO_STRING(
        ARRAY_CONCAT(
          IF(ARRAY_LENGTH(t.airflow_errors) > 0, [CONCAT('-----airflow_errors-----\n', ARRAY_TO_STRING(t.airflow_errors, '\n'))], []),
          IF(ARRAY_LENGTH(t.airflow_keywords) > 0, [CONCAT('-----airflow_keywords-----\n', ARRAY_TO_STRING(t.airflow_keywords, '\n'))], []),
          IF(ARRAY_LENGTH(t.k8s_messages) > 0, [CONCAT('-----k8s_messages-----\n', ARRAY_TO_STRING(t.k8s_messages, '\n'))], [])
        ),
        '\n'
      ) AS logs_string,
      t.tasks
    FROM
      `cienet-cmcs.amy_xlml_poc_prod.dag_runs_with_logs` AS dr,
      UNNEST(dr.tests) AS t
  ),

  ---
  
  -- 2. error_code_lookup CTE: Combines all logs and joins to config_error table to assign codes.
  error_code_lookup AS (
    SELECT
      bvd.dag_id,
      bvd.run_id,
      bvd.test_id,
      -- Aggregate the DISTINCT matching error codes
      ARRAY_AGG(DISTINCT cfg.err_code IGNORE NULLS ORDER BY cfg.err_code DESC) AS err_codes
    FROM
      base_view_data AS bvd,
      -- Unnest all logs into a single column for joining
      UNNEST(ARRAY_CONCAT(bvd.airflow_errors, bvd.airflow_keywords, bvd.k8s_messages)) AS log_message
    INNER JOIN
      `amy_xlml_poc_prod.config_error` AS cfg
      ON REGEXP_CONTAINS(log_message, cfg.err_regx)
    GROUP BY
      1, 2, 3
  )

---

-- 3. Final SELECT: Join the base data back to the error codes
SELECT
  bvd.dag_id,
  bvd.run_id,
  ANY_VALUE(bvd.execution_date) AS execution_date,
  ANY_VALUE(bvd.run_start_date) AS run_start_date,
  ANY_VALUE(bvd.run_end_date) AS run_end_date,
  ANY_VALUE(bvd.run_status) AS run_status,
  -- Re-nest the test data back into the ARRAY structure
  ARRAY_AGG(
    STRUCT(
      bvd.test_id,
      bvd.test_start_date,
      bvd.test_end_date,
      bvd.test_status,
      bvd.workload_id,
      bvd.cluster_project,
      bvd.cluster_name,
      bvd.cluster_region,
      bvd.accelerator_type,
      bvd.accelerator_family,
      bvd.machine_families,
      bvd.logs_string,
      bvd.log_url_error,
      bvd.log_url_all,
      bvd.log_url_k8s,
      bvd.log_url_graph,
      IFNULL(ecl.err_codes, []) AS err_codes, -- Final error codes
      ARRAY_TO_STRING(
          ARRAY(SELECT CAST(code AS STRING) FROM UNNEST(IFNULL(ecl.err_codes, [])) AS code),
          ','
      ) AS err_codes_string,     
      bvd.tasks
    ) 
  ) AS tests
FROM
  base_view_data AS bvd
LEFT JOIN
  error_code_lookup AS ecl
  ON bvd.dag_id = ecl.dag_id
  AND bvd.run_id = ecl.run_id
  AND bvd.test_id = ecl.test_id
GROUP BY dag_id, run_id

