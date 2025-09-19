CREATE OR REPLACE VIEW
  `cienet-cmcs.amy_xlml_poc_prod.dag_runs_with_logs_view` AS
SELECT
  dr.dag_id,
  dr.run_id,
  dr.execution_date,
  dr.run_start_date,
  dr.run_end_date,
  dr.run_status,
  ARRAY(
  SELECT
    AS STRUCT 
    t.test_id, 
    t.test_start_date, 
    t.test_end_date, 
    t.test_status, 
    workload_id, 
    cluster_project, 
    cluster_name, 
    accelerator_type,
    accelerator_family,
    machine_families,
    t.tasks,
    ARRAY_TO_STRING(
        ARRAY_CONCAT(
          -- Check if airflow_errors is not empty, then format it with a header
          IF(ARRAY_LENGTH(t.airflow_errors) > 0, [CONCAT('-----airflow_errors-----\n', ARRAY_TO_STRING(t.airflow_errors, '\n'))], []),
          -- Check if airflow_keywords is not empty, then format it with a header
          IF(ARRAY_LENGTH(t.airflow_keywords) > 0, [CONCAT('-----airflow_keywords-----\n', ARRAY_TO_STRING(t.airflow_keywords, '\n'))], []),
          -- Check if k8s_messages is not empty, then format it with a header
          IF(ARRAY_LENGTH(t.k8s_messages) > 0, [CONCAT('-----k8s_messages-----\n', ARRAY_TO_STRING(t.k8s_messages, '\n'))], [])
        ),
        -- Join the formatted strings with a newline
        '\n'
      ) AS logs_string,
  t.log_url_error, t.log_url_all, t.log_url_k8s,log_url_graph
  FROM
    UNNEST(dr.tests) AS t ) AS tests
FROM
  `cienet-cmcs.amy_xlml_poc_prod.dag_runs_with_logs` AS dr;
