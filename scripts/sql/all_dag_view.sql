CREATE OR REPLACE VIEW `amy_xlml_poc_prod.all_dag_view` AS
WITH 
all_dags AS (
  SELECT * FROM `amy_xlml_poc_prod.all_dag_base_view` 
),

all_dag_with_run AS (
  SELECT dag_id, dag_owners, schedule_interval, formatted_schedule, is_paused, tags, category, accelerator, 
    total_runs AS total_runs_all, total_runs_iq, passed_runs, partial_passed_runs, failed_runs, quarantined_runs, 
    total_tests, total_tests_qe, total_tests_q, test_ids, test_ids_qe, test_ids_q,
    last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),
statistic AS (
  SELECT
    w.window_value,
    s.dag_id,
    s.total_runs,
    s.passed_runs,
    s.partial_passed_runs,
    s.runs_pass_rate,
    s.runs_partial_pass_rate
  FROM 
    `amy_xlml_poc_prod.statistic_last_window` as w,
    UNNEST(w.dag_statistic) as s 
  WHERE window_name like 'last_%d'
)

SELECT d.dag_id, 
  s.is_paused, 
  d.description, 
  s.schedule_interval, 
  s.formatted_schedule,   
  s.tags, 
  s.category, 
  s.accelerator, 
  s.dag_owners, 
  s.total_runs_all, s.passed_runs, s.partial_passed_runs, 
  s.total_tests, s.total_tests_qe, s.total_tests_q,
  s.test_ids, s.test_ids_qe, s.test_ids_q,
  s.last_exec, s.last_succ,
  s30.total_runs total_runs_30,
  s30.passed_runs passed_runs_30,
  s30.partial_passed_runs partial_passed_runs_30,
  s30.runs_pass_rate runs_pass_rate_30,
  s30.runs_partial_pass_rate runs_partial_pass_rate_30,
  s1.total_runs total_runs_1,
  s1.passed_runs passed_runs_1,
  s1.partial_passed_runs partial_passed_runs_1,
  s1.runs_pass_rate runs_pass_rate_1,
  s1.runs_partial_pass_rate runs_partial_pass_rate_1,
  s3.total_runs total_runs_3,
  s3.passed_runs passed_runs_3,
  s3.partial_passed_runs partial_passed_runs_3,
  s3.runs_pass_rate runs_pass_rate_3,
  s3.runs_partial_pass_rate runs_partial_pass_rate_3,
  s7.total_runs total_runs_7,
  s7.passed_runs passed_runs_7,
  s7.partial_passed_runs partial_passed_runs_7,
  s7.runs_pass_rate runs_pass_rate_7,
  s7.runs_partial_pass_rate runs_partial_pass_rate_7  
FROM all_dags d
LEFT JOIN all_dag_with_run s ON d.dag_id = s.dag_id
LEFT JOIN statistic s1 ON d.dag_id = s1.dag_id AND s1.window_value = 1
LEFT JOIN statistic s3 ON d.dag_id = s3.dag_id AND s3.window_value = 3
LEFT JOIN statistic s7 ON d.dag_id = s7.dag_id AND s7.window_value = 7
LEFT JOIN statistic s30 ON d.dag_id = s30.dag_id AND s30.window_value = 30


