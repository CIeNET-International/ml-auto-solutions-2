CREATE OR REPLACE VIEW `amy_xlml_poc_prod.quarantine_info_view` AS
WITH
quarantined_dags AS (
  SELECT dag_id
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` 
  WHERE total_tests = total_tests_q

),
quarantined_tests AS (
  SELECT dag_id, test_id
  FROM `amy_xlml_poc_prod.base` b, UNNEST(test_ids_q) test_id
),

all_tests_info AS (
  SELECT b.dag_id, test_id,
    CASE WHEN total_tests = total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,
    CASE WHEN test_id IN UNNEST(test_ids_q) THEN TRUE ELSE FALSE END AS is_quarantined_test
  FROM `amy_xlml_poc_prod.base` b, UNNEST(test_ids) test_id
)

SELECT
  (SELECT ARRAY_AGG(dag_id) FROM quarantined_dags) AS quarantined_dags,
  (SELECT ARRAY_AGG(STRUCT(dag_id, test_id)) FROM quarantined_tests) AS quarantined_tests, 
  (SELECT ARRAY_AGG(STRUCT(dag_id, test_id, is_quarantined_dag, is_quarantined_test)) FROM all_tests_info) AS all_tests_info

