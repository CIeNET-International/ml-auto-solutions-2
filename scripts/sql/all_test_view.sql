CREATE OR REPLACE VIEW `amy_xlml_poc_prod.all_test_view` AS
WITH 
all_dags AS (
  SELECT * FROM `amy_xlml_poc_prod.all_dag_base` 
),

filtered_tests AS (
  SELECT w.dag_id, w.test_id, w.test_is_passed, w.test_start_date, w.test_end_date,
    c.cluster_name, c.project_name, c.region, FALSE AS is_quarantined
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name='the_r_1'
),

filtered_tests_qr_qt AS (
  SELECT w.dag_id, w.test_id, w.test_is_passed, w.test_start_date, w.test_end_date, 
    c.cluster_name, c.project_name, c.region, TRUE AS is_quarantined
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests_qr_qt) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name='the_r_1'
),

last_run_tests AS (
  SELECT * FROM filtered_tests
  UNION ALL
  SELECT * FROM filtered_tests_qr_qt
),

last_3r AS (
  SELECT w.dag_id, w.test_id, SUM(w.test_is_passed) AS passed, COUNT(distinct w.run_id) AS total_run_tests,
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name LIKE 'the_r_%' and window_value <= 3 GROUP BY w.dag_id, w.test_id
),

last_3r_qr_qt AS (
  SELECT w.dag_id, w.test_id, SUM(w.test_is_passed) AS passed, COUNT(distinct w.run_id) AS total_run_tests,
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests_qr_qt) w
  LEFT JOIN `amy_xlml_poc_prod.cluster_info_view_latest` c ON w.dag_id = c.dag_id AND w.test_id = c.test_id
  WHERE window_name LIKE 'the_r_%' and window_value <= 3 GROUP BY w.dag_id, w.test_id
),

last_3r_tests_status AS (
  SELECT * FROM last_3r
  UNION ALL
  SELECT * FROM last_3r_qr_qt
),

statistic_r AS (
  SELECT
    s.dag_id,
    s.passed_runs,
    s.partial_passed_runs,
    s.runs_pass_rate,
    s.runs_partial_pass_rate
  FROM 
    `amy_xlml_poc_prod.statistic_last_window` as w,
    UNNEST(w.dag_statistic) as s 
  WHERE window_name = 'last_3r'
)

SELECT t.dag_id, t.test_id, t.test_is_passed, t.test_start_date, t.test_end_date,
  t.cluster_name, t.project_name, t.region, t.is_quarantined,
  s.total_run_tests AS total_run_tests_3r,
  s.passed AS passed_tests_3r,
  SAFE_DIVIDE(s.passed, s.total_run_tests) AS tests_pass_rate_3r,
  r.passed_runs AS passed_runs_3r, 
  r.partial_passed_runs AS partial_passed_runs_3r, 
  r.runs_pass_rate AS runs_pass_rate_3r, 
  r.runs_partial_pass_rate AS runs_partial_pass_rate_3r,
  a.category, a.accelerator, a.tags, a.dag_owners, a.is_paused, a.formatted_schedule
FROM last_run_tests t
JOIN all_dags a ON t.dag_id = a.dag_id
LEFT JOIN statistic_r r ON t.dag_id = r.dag_id
LEFT JOIN last_3r_tests_status s ON t.dag_id = s.dag_id AND t.test_id = s.test_id

