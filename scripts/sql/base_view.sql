CREATE OR REPLACE VIEW `amy_xlml_poc_prod.base_view` AS
WITH 
all_dags AS (
  SELECT * FROM `amy_xlml_poc_prod.all_dag_base_view`
),

dag_runs_ended AS (
  SELECT dr.dag_id, dr.run_id, dr.execution_date, dr.start_date, dr.end_date
  FROM `amy_xlml_poc_prod.dag_run` dr
  JOIN all_dags dag ON dag.dag_id=dr.dag_id
  --WHERE dag.is_active = TRUE
  --WHERE dag.is_paused = FALSE
  WHERE dr.start_date is not null and dr.end_date is not null
    --AND start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY) AND CURRENT_TIMESTAMP()
   AND DATE(start_date,'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 30 DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
   AND dr.dag_id NOT IN (SELECT dag_id from `amy_xlml_poc_prod.config_ignore_dags`)
),
    
-- all tasks status
last_task_status AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.task_id,
    ti.state,
    ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn,
    ti.start_date,
    ti.end_date,
    SPLIT(ti.task_id, '.')[OFFSET(0)] AS test_id,
    IFNULL(q.is_quarantine, FALSE) AS is_quarantine
  FROM
    `amy_xlml_poc_prod.task_instance` AS ti
  JOIN dag_runs_ended AS dr ON ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id
  LEFT JOIN `amy_xlml_poc_prod.quarantine_view` q ON ti.dag_id=q.dag_id and SPLIT(ti.task_id, '.')[OFFSET(0)] =q.test_id
),    

--tasks quarantine excluded  
last_task_status_qe AS (
  SELECT dag_id, run_id, task_id, state, rn, start_date, end_date, test_id
  FROM last_task_status
  WHERE is_quarantine=false
),

--tasks quarantined 
last_task_status_quarantine AS (
  SELECT dag_id, run_id, task_id, state, rn, start_date, end_date, test_id
  FROM last_task_status
  WHERE is_quarantine=true
),

--dag_run quarantine excluded 
--dag_runs_ended_qe AS (
--  SELECT r.dag_id, r.run_id, execution_date, r.start_date, r.end_date
--  FROM dag_runs_ended r
--  JOIN last_task_status_qe t ON r.dag_id=t.dag_id AND r.run_id=t.run_id
--),

-- Test-level aggregation per run for all tasks last_tried include quarantine
tasks_test AS (
  SELECT
    ti.dag_id,
    ti.run_id,
    ti.test_id,
    ti.task_id,
    ti.state,
    ti.start_date,
    ti.end_date,
    ti.is_quarantine
  FROM last_task_status AS ti
  WHERE rn = 1
),

--grouping tasks count (total, successful) to test_id
task_test_status_pre AS (
  SELECT
    dag_id,
    run_id,
    test_id,
    COUNT(*) AS total_tasks,
    SUM(CASE WHEN state = 'success' OR 
          (state = 'skipped' AND dag_id IN (
             SELECT dag_id FROM `cienet-cmcs.amy_xlml_poc_prod.config_ignore_skipped_dags`
          )) 
        THEN 1 ELSE 0 END) AS successful_tasks, 
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date,
    ANY_VALUE(is_quarantine) AS is_quarantine
  FROM tasks_test
  GROUP BY dag_id, run_id, test_id
),

-- calculate test_is_passed by task counts
task_test_status AS (
  SELECT
    p.dag_id,
    p.run_id,
    p.test_id,
    p.total_tasks,
    p.successful_tasks,
    CASE WHEN p.total_tasks = p.successful_tasks THEN 1 ELSE 0 END AS test_is_passed,   
    p.start_date,
    p.end_date,
    p.is_quarantine
  FROM task_test_status_pre p
),

--aggregate to run_id
test_status AS (
  SELECT
    dag_id,
    run_id,
    --COUNT(*) AS total_tests,
    --SUM(test_is_passed) AS successful_tests,
    --SUM(total_tasks) AS total_tasks,
    --SUM(successful_tasks) AS successful_tasks,
    
    SUM(CASE WHEN is_quarantine = FALSE THEN 1 ELSE 0 END) AS total_tests,
    SUM(CASE WHEN is_quarantine = FALSE THEN test_is_passed ELSE 0 END) AS successful_tests,
    SUM(CASE WHEN is_quarantine = FALSE THEN 1 ELSE 0 END) - SUM(CASE WHEN is_quarantine = FALSE THEN test_is_passed ELSE 0 END)AS failed_tests,
    SUM(CASE WHEN is_quarantine = FALSE THEN total_tasks ELSE 0 END) AS total_tasks,
    SUM(CASE WHEN is_quarantine = FALSE THEN successful_tasks ELSE 0 END) AS successful_tasks,

    SUM(CASE WHEN is_quarantine = TRUE THEN 1 ELSE 0 END) AS  total_tests_q,
    SUM(CASE WHEN is_quarantine = TRUE THEN test_is_passed ELSE 0 END) AS successful_tests_q,
    SUM(CASE WHEN is_quarantine = TRUE THEN 1 ELSE 0 END) - SUM(CASE WHEN is_quarantine = FALSE THEN test_is_passed ELSE 0 END)AS failed_tests_q,
    SUM(CASE WHEN is_quarantine = TRUE THEN total_tasks ELSE 0 END) AS total_tasks_q,
    SUM(CASE WHEN is_quarantine = TRUE THEN successful_tasks ELSE 0 END) AS successful_tasks_q,
  FROM task_test_status
  GROUP BY dag_id, run_id  
),

-- Run-level pass/fail status for all runs, the pass/fail calculated with quarantine excluded tests
-- filtered out runs without any non-quarantine tests
dag_run_base_pre AS (
  SELECT 
    t1.dag_id, 
    t1.run_id,
    t3.execution_date,
    t3.start_date,
    t3.end_date,
    ROW_NUMBER() OVER (PARTITION BY t3.dag_id ORDER BY t3.start_date DESC) AS run_order_desc, 
    t1.total_tests,
    t1.successful_tests,
    t1.failed_tests,
    t1.total_tasks,
    t1.successful_tasks,
    CASE WHEN total_tasks = successful_tasks THEN 1 ELSE 0 END AS is_passed,
    CASE WHEN successful_tests > 0 AND successful_tests < total_tests THEN 1 ELSE 0 END AS is_partial_passed,
    t1.total_tests_q,
    t1.successful_tests_q,
    t1.failed_tests_q,
    t1.total_tasks_q,
    t1.successful_tasks_q
  FROM test_status t1
  JOIN dag_runs_ended t3 ON t1.dag_id = t3.dag_id AND t1.run_id = t3.run_id
    WHERE t1.total_tests > 0
),

-- add is_passed_run_order_desc to runs with non-quarantined tests
dag_run_base AS (
  SELECT 
    t1.dag_id, 
    t1.run_id, 
    t1.execution_date, 
    t1.start_date, 
    t1.end_date, 
    t1.run_order_desc, 
    t1.total_tests, 
    t1.successful_tests, 
    t1.failed_tests,
    t1.total_tasks, 
    t1.successful_tasks, 
    t1.is_passed, 
    t1.is_partial_passed,
    ROW_NUMBER() OVER (PARTITION BY t1.dag_id, t1.is_passed ORDER BY t1.run_order_desc) AS is_passed_run_order_desc,
    t1.total_tests_q,
    t1.successful_tests_q,
    t1.failed_tests_q,
    t1.total_tasks_q,
    t1.successful_tasks_q    
  FROM dag_run_base_pre t1
),

--count runs without any non-quarantine tests
quarantine_run_counts AS (
  SELECT
    dag_id,
    COUNT(DISTINCT run_id) AS quarantine_runs_count
  FROM
    test_status
  WHERE 
    total_tests = 0
  GROUP BY
    dag_id
),

--aggregate run related counts
dag_run_cnt_pre AS (
  SELECT 
    t1.dag_id, 
    COUNT(DISTINCT t1.run_id) AS total_runs,
    SUM(t1.is_passed) AS passed_runs,
    SUM(t1.is_partial_passed) AS partial_passed_runs,

    IFNULL(t2.quarantine_runs_count, 0) AS quarantined_runs
  FROM dag_run_base AS t1 
  LEFT JOIN
    quarantine_run_counts AS t2 ON t1.dag_id = t2.dag_id
  GROUP BY
    t1.dag_id,
    t2.quarantine_runs_count
),

dag_run_cnt AS (
  SELECT 
    dag_id,
    total_runs,
    passed_runs,
    partial_passed_runs,
    quarantined_runs,
    total_runs + quarantined_runs AS total_runs_iq,
    total_runs - passed_runs - partial_passed_runs AS failed_runs
  FROM dag_run_cnt_pre
),

last_exec AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date
  FROM dag_run_base
  WHERE run_order_desc=1
),

last_succ AS (
  SELECT dag_id, run_id, execution_date, start_date, end_date
  FROM dag_run_base
  WHERE is_passed=1 AND is_passed_run_order_desc=1
),

ref_dag_run AS (
  SELECT dag_id,run_id 
  FROM dag_run_base
  WHERE (is_passed=1 AND is_passed_run_order_desc=1) 
    OR run_order_desc=1 
), 

-- list test_id for dag, considerate runs (last_succ,last_exec)
top_level_tests AS (
  SELECT 
    t.dag_id,
    COUNT(DISTINCT test_id) AS total_tests,
    COUNT(DISTINCT CASE WHEN is_quarantine = FALSE THEN test_id END) AS total_tests_qe,
    COUNT(DISTINCT CASE WHEN is_quarantine = TRUE THEN test_id END) AS total_tests_q,
    ARRAY_AGG(DISTINCT test_id) AS test_ids,
    ARRAY_AGG(
      DISTINCT CASE WHEN NOT t.is_quarantine THEN t.test_id ELSE NULL END 
      IGNORE NULLS
    ) AS test_ids_qe,
    ARRAY_AGG(
      DISTINCT CASE WHEN t.is_quarantine THEN t.test_id ELSE NULL END 
      IGNORE NULLS
    ) AS test_ids_q
  FROM last_task_status t
  GROUP BY t.dag_id
),

-- Aggregates non-quarantined test details into a nested array for each run
test_details_per_run AS (
  SELECT
    s.dag_id,
    s.run_id,
    ARRAY_AGG(STRUCT(s.test_id, s.total_tasks AS test_total_tasks, s.successful_tasks AS test_successful_tasks, s.start_date, s.end_date,
     s.test_is_passed AS is_passed)) AS tests,
  FROM task_test_status s
  JOIN dag_run_base b ON s.dag_id=b.dag_id AND s.run_id=b.run_id
    WHERE is_quarantine = FALSE
  GROUP BY dag_id, run_id
),

-- Aggregates run details into an array of structs, including the nested tests
all_run_details AS (
  SELECT
    drb.dag_id,
    ARRAY_AGG(
      STRUCT(
        drb.run_id,
        drb.execution_date,
        drb.start_date,
        drb.end_date,
        drb.is_passed,
        drb.is_partial_passed,
        drb.total_tests,
        drb.successful_tests,
        drb.total_tasks,
        drb.successful_tasks,
        tdpr.tests AS tests,
        drb.run_order_desc,
        drb.is_passed_run_order_desc,
        drb.total_tests_q,
        drb.successful_tests_q,
        drb.total_tasks_q,
        drb.successful_tasks_q
       ) ORDER BY drb.run_order_desc
    ) AS runs
  FROM dag_run_base drb
  JOIN test_details_per_run tdpr ON drb.dag_id = tdpr.dag_id AND drb.run_id = tdpr.run_id
  GROUP BY drb.dag_id
)

SELECT
  d.dag_id,
  d.dag_owners,
  d.schedule_interval,
  REGEXP_REPLACE(d.formatted_schedule, r'^"|"$', '') AS formatted_schedule,
  d.is_paused,
  d.tags,
  d.category,
  d.accelerator,
  d.description,
  tlt.total_tests AS total_tests,
  tlt.total_tests_qe AS total_tests_qe,
  tlt.total_tests_q AS total_tests_q,
  tlt.test_ids,
  tlt.test_ids_qe,
  tlt.test_ids_q,
  cnt.total_runs,
  cnt.passed_runs,
  cnt.partial_passed_runs,
  cnt.failed_runs,
  cnt.quarantined_runs,
  cnt.total_runs_iq,
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',le.start_date) AS last_exec,  
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S',ls.start_date) AS last_succ, 
  ard.runs AS runs
FROM all_dags d
LEFT JOIN top_level_tests tlt ON d.dag_id = tlt.dag_id
LEFT JOIN all_run_details ard ON d.dag_id = ard.dag_id
LEFT JOIN dag_run_cnt cnt ON d.dag_id = cnt.dag_id
LEFT JOIN last_exec le ON d.dag_id = le.dag_id
LEFT JOIN last_succ ls ON d.dag_id = ls.dag_id
ORDER BY d.dag_id
