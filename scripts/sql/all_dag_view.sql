CREATE OR REPLACE VIEW `amy_xlml_poc_prod.all_dag_view` AS
WITH 
all_dags AS (
  SELECT d.*
  FROM `amy_xlml_poc_prod.dag` d
  WHERE dag_id IN
  (SELECT dag_id FROM `cienet-cmcs.amy_xlml_poc_prod.serialized_dag`)
),

all_dag_with_run AS (
  SELECT dag_id, dag_owners, schedule_interval, formatted_schedule, is_paused, total_runs AS total_runs_all, passed_runs, total_tests, tags, category, accelerator, 
    last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),
statistic AS (
  SELECT
    w.window_value,
    s.dag_id,
    s.total_runs,
    s.passed_runs,
    s.runs_pass_rate
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
  s.total_runs_all, s.passed_runs, s.total_tests, s.last_exec, s.last_succ,
  s1.total_runs total_runs_1,
  s1.passed_runs passed_runs_1,
  s1.runs_pass_rate runs_pass_rate_1,
  s3.total_runs total_runs_3,
  s3.passed_runs passed_runs_3,
  s3.runs_pass_rate runs_pass_rate_3,
  s7.total_runs total_runs_7,
  s7.passed_runs passed_runs_7,
  s7.runs_pass_rate runs_pass_rate_7  
FROM all_dags d
LEFT JOIN all_dag_with_run s ON d.dag_id = s.dag_id
LEFT JOIN statistic s1 ON d.dag_id = s1.dag_id AND s1.window_value = 1
LEFT JOIN statistic s3 ON d.dag_id = s3.dag_id AND s3.window_value = 3
LEFT JOIN statistic s7 ON d.dag_id = s7.dag_id AND s7.window_value = 7





