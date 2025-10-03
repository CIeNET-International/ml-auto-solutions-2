
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.statistic_last_window_view` AS
WITH

windows_d AS (
  SELECT value,CONCAT('last_', value, 'd') AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='d'
),
windows_r AS (
  SELECT value,CONCAT('last_', value, 'r') AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='r'
),
windows_r_the AS (
  SELECT value,CONCAT('the_r_', value) AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='the_r'
),
windows_d_the AS (
  SELECT value,CONCAT('the_d_', value) AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='the_d'
),
windows_w_the AS (
  SELECT value,CONCAT('the_w_', value) AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='the_w'
),


all_dags AS (
  SELECT dag_id, dag_owners, formatted_schedule, is_paused, total_runs AS total_runs_all, passed_runs, total_tests, tags, category, accelerator, last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

-- Filter last N UTC days
filtered_runs_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  --WHERE start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP()
  JOIN windows_d w ON DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
  --JOIN windows_d w ON DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND CURRENT_DATE('UTC')
), 

filtered_tests_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_d w ON DATE(runs.start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
  --JOIN windows_d w ON DATE(runs.start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND CURRENT_DATE('UTC')
), 


-- Filter last N runs
filtered_runs_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_r w ON runs.run_order_desc <= w.value
), 

filtered_tests_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_r w ON runs.run_order_desc <= w.value
), 

-- Filter the Nth run
filtered_runs_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_r_the w ON runs.run_order_desc = w.value
), 

filtered_tests_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_r_the w ON runs.run_order_desc = w.value
), 

-- Filter the Nth day
filtered_runs_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
), 

filtered_tests_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
), 

-- Filter the Nth week
filtered_runs_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
), 

filtered_tests_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.run_order_desc, runs.is_passed_run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)), 

filtered_runs AS (
  SELECT * from filtered_runs_d
  UNION ALL
  SELECT * from filtered_runs_r
  UNION ALL
  SELECT * from filtered_runs_r_the
  UNION ALL
  SELECT * from filtered_runs_d_the
  UNION ALL
  SELECT * from filtered_runs_w_the  
),

filtered_tests AS (
  SELECT * from filtered_tests_d
  UNION ALL
  SELECT * from filtered_tests_r
  UNION ALL
  SELECT * from filtered_tests_r_the
  UNION ALL
  SELECT * from filtered_tests_d_the
  UNION ALL
  SELECT * from filtered_tests_w_the  
),

-- DAG run statistic 
dag_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, dag_id, ANY_VALUE(category) AS category, ANY_VALUE(accelerator) AS accelerator,
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    ANY_VALUE(total_tests) AS number_of_tests,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, dag_id
), 

-- DAG test statistic 
dag_tests_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value), dag_id,
    COUNT(test_id) AS total_run_tests,
    SUM(test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate,
  FROM filtered_tests
  GROUP BY window_name, dag_id
), 

-- DAG runs/tests statictic
dag_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.dag_id, r.category, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, r.number_of_tests, 
    min_run_start, max_run_end,
    t.total_run_tests, t.passed_tests, t.tests_pass_rate,
    d.formatted_schedule, d.is_paused, d.tags, d.total_runs_all, d.dag_owners AS dag_owner, d.last_exec, d.last_succ
  FROM dag_statistic r
  LEFT JOIN dag_tests_statistic t ON r.window_name = t.window_name AND r.dag_id = t.dag_id
  LEFT JOIN all_dags d ON r.dag_id = d.dag_id
),

-- statistic category
category_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, 
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, category
), 
category_tests_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, 
    COUNT(test_id) AS total_run_tests,
    SUM(test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate,
  FROM filtered_tests
  GROUP BY window_name, category
), 

category_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.category, r.total_runs, r.passed_runs, r.runs_pass_rate, min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate
  FROM category_statistic r
  LEFT JOIN category_tests_statistic t ON r.window_name = t.window_name AND r.category = t.category
),

-- statistic accelerator
accelerator_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, accelerator, 
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, accelerator
), 

accelerator_tests_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, accelerator, 
    COUNT(test_id) AS total_run_tests,
    SUM(test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate,
  FROM filtered_tests
  GROUP BY window_name, accelerator
), 

accelerator_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate
  FROM accelerator_statistic r
  LEFT JOIN accelerator_tests_statistic t ON r.window_name = t.window_name AND r.accelerator = t.accelerator
),


-- statistic category,accelerator
category_accelerator_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, accelerator,
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, category, accelerator
), 
category_accelerator_tests_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, accelerator,
    COUNT(test_id) AS total_run_tests,
    SUM(test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate,
  FROM filtered_tests
  GROUP BY window_name, category, accelerator
), 

category_accelerator_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.category, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate
  FROM category_accelerator_statistic r
  LEFT JOIN category_accelerator_tests_statistic t ON r.window_name = t.window_name AND r.category = t.category AND r.accelerator = t.accelerator
),


--cluster 
dag_tests_cluster AS (
  SELECT dag_id, test_id, cluster_name, project_name, accelerator_type, accelerator_family
  FROM `amy_xlml_poc_prod.cluster_info_view_latest`
),

-- DAG test statistic by cluster
dag_tests_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, c.project_name, c.cluster_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name, c.cluster_name
), 

-- DAG test category statistic by cluster
dag_tests_category_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, t.category, c.project_name, c.cluster_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.category, c.project_name, c.cluster_name
), 

-- DAG test accelerator statistic by cluster
dag_tests_accelerator_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, t.accelerator, c.project_name, c.cluster_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.accelerator, c.project_name, c.cluster_name
), 

-- DAG test statistic by project
dag_tests_statistic_project AS (    
  SELECT 
    t.window_name, ANY_VALUE(t.window_value) AS window_value,c.project_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name
),

dag_tests_category_statistic_project AS (    
  SELECT 
    t.window_name, ANY_VALUE(t.window_value) AS window_value, t.category, c.project_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.category, c.project_name
),

dag_tests_accelerator_statistic_project AS (    
  SELECT 
    t.window_name, ANY_VALUE(t.window_value) AS window_value, t.accelerator, c.project_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.accelerator, c.project_name
),

dag_run_statistic_category_array AS (
SELECT 
  cs.window_name, cs.window_value, cs.category, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end,
  ARRAY(SELECT STRUCT(ds.accelerator, ds.dag_id, ds.total_runs, ds.passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name AND ds.category=cs.category) AS dag_statistic
FROM category_runs_tests_statistic cs
),

dag_run_statistic_accelerator_array AS (
SELECT 
  cs.window_name, cs.window_value, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end,
  ARRAY(SELECT STRUCT(ds.category, ds.dag_id, ds.total_runs, ds.passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name AND ds.accelerator=cs.accelerator) AS dag_statistic
FROM accelerator_runs_tests_statistic cs
),

dag_run_statistic_category_accelerator_array AS (
SELECT 
  cs.window_name, cs.window_value, cs.category, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end,
  ARRAY(SELECT STRUCT(ds.dag_id, ds.total_runs, ds.passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name AND ds.category=cs.category AND ds.accelerator=cs.accelerator) AS dag_statistic
FROM category_accelerator_runs_tests_statistic cs
),


t1 AS (
  SELECT d.window_name, ANY_VALUE(d.window_value) AS window_value,
  ARRAY_AGG(STRUCT(d.dag_id, d.category, d.accelerator, d.total_runs, d.passed_runs, d.runs_pass_rate, d.number_of_tests, min_run_start, max_run_end, d.total_run_tests, d.passed_tests, d.tests_pass_rate, d.formatted_schedule, d.is_paused, d.tags, d.total_runs_all, d.dag_owner, d.last_exec, d.last_succ)) AS dag_statistic 
  FROM dag_runs_tests_statistic d
  GROUP by d.window_name
),

t2 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.category, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, cs.dag_statistic)) AS dag_category_statistic
  FROM dag_run_statistic_category_array cs
  GROUP BY cs.window_name
),

t3 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, cs.dag_statistic)) AS dag_accelerator_statistic
  FROM dag_run_statistic_accelerator_array cs
  GROUP BY cs.window_name
),

t4 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.category, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, cs.dag_statistic)) AS dag_category_accelerator_statistic
  FROM dag_run_statistic_category_accelerator_array cs
  GROUP BY cs.window_name
),

t5 AS (
  SELECT c.window_name, ANY_VALUE(c.window_value) AS window_value,
    ARRAY_AGG(STRUCT(c.project_name, c.cluster_name, min_run_start, max_run_end, c.total_run_tests, c.passed_tests, c.tests_pass_rate)) AS cluster_statistic
  FROM dag_tests_statistic_cluster c
  GROUP BY c.window_name
),

t6 AS (
  SELECT p.window_name, ANY_VALUE(p.window_value) AS window_value,
    ARRAY_AGG(STRUCT(p.project_name, min_run_start, max_run_end, p.total_run_tests, p.passed_tests, p.tests_pass_rate)) AS project_statistic
  FROM dag_tests_statistic_project p
  GROUP BY p.window_name
)

SELECT 
  d.window_name,    
  d.window_value,
  d.dag_statistic,
  cat.dag_category_statistic,
  acc.dag_accelerator_statistic,
  ca.dag_category_accelerator_statistic,
  c.cluster_statistic,
  p.project_statistic
FROM  t1 d
LEFT JOIN t2 cat ON d.window_name = cat.window_name
LEFT JOIN t3 acc ON d.window_name = acc.window_name
LEFT JOIN t4 ca ON d.window_name = ca.window_name
LEFT JOIN t5 c ON d.window_name = c.window_name
LEFT JOIN t6 p ON d.window_name = p.window_name




