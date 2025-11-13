
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.statistic_last_window_view` AS
WITH
windows_range_desc AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.windows_range_desc) w
),

all_dags AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.all_dags) w
),

filtered_runs AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_runs) w
),

filtered_runs_qr AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_runs_qr) w
),

filtered_tests AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests) w
),

filtered_tests_qr_qt AS (
  SELECT w.*
  FROM `amy_xlml_poc_prod.statistic_data` v,unnest (v.filtered_tests_qr_qt) w
),

-- DAG run statistic 
dag_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, dag_id, ANY_VALUE(category) AS category, ANY_VALUE(accelerator) AS accelerator,
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    ANY_VALUE(total_tests) AS number_of_tests,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, dag_id
), 

qr_statistic AS (    
  SELECT 
    window_name, 
    COUNT(run_id) AS total_runs_qr,
  FROM filtered_runs_qr
  GROUP BY window_name
), 
qr_category_statistic AS (    
  SELECT 
    window_name, category,
    COUNT(run_id) AS total_runs_qr,
  FROM filtered_runs_qr
  GROUP BY window_name, category 
), 
qr_accelerator_statistic AS ( 
  SELECT 
    window_name, accelerator,
    COUNT(run_id) AS total_runs_qr,
  FROM filtered_runs_qr
  GROUP BY window_name, accelerator 
),
qr_category_accelerator_statistic AS (    
  SELECT 
    window_name, category, accelerator,
    COUNT(run_id) AS total_runs_qr,
  FROM filtered_runs_qr
  GROUP BY window_name, category, accelerator 
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
  SELECT r.window_name, r.window_value, r.dag_id, r.category, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, 
    r.partial_passed_runs, r.runs_partial_pass_rate, 
    r.number_of_tests, 
    min_run_start, max_run_end,
    t.total_run_tests, t.passed_tests, t.tests_pass_rate,
    d.formatted_schedule, d.is_paused, d.tags, d.total_runs_all, d.dag_owners AS dag_owner, d.last_exec, d.last_succ
  FROM dag_statistic r
  LEFT JOIN dag_tests_statistic t ON r.window_name = t.window_name AND r.dag_id = t.dag_id
  LEFT JOIN all_dags d ON r.dag_id = d.dag_id
),

-- statistic category
category_statistic_pre AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, 
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, category
), 

category_statistic AS (    
  SELECT 
    p.window_name, window_value, p.category, 
    total_runs,
    passed_runs,
    runs_pass_rate,
    partial_passed_runs,
    runs_partial_pass_rate,    
    min_run_start, max_run_end,
    q.total_runs_qr
  FROM category_statistic_pre p
  LEFT JOIN qr_category_statistic q ON p.window_name = q.window_name AND 
    COALESCE(p.category, '___NULL_CATEGORY_KEY___') = COALESCE(q.category, '___NULL_CATEGORY_KEY___')
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
category_tests_qr_qt_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, 
    COUNT(test_id) AS total_run_tests_q,
    SUM(test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate_q,
  FROM filtered_tests_qr_qt
  GROUP BY window_name, category
), 

category_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.category, r.total_runs, r.passed_runs, r.runs_pass_rate, 
    r.partial_passed_runs, r.runs_partial_pass_rate, r.total_runs_qr,
    min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate,
    q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM category_statistic r
  LEFT JOIN category_tests_statistic t ON r.window_name = t.window_name 
    AND COALESCE(r.category, '___NULL_CATEGORY_KEY___') = COALESCE(t.category, '___NULL_CATEGORY_KEY___')
  LEFT JOIN category_tests_qr_qt_statistic q ON r.window_name = q.window_name 
    AND COALESCE(r.category, '___NULL_CATEGORY_KEY___') = COALESCE(q.category, '___NULL_CATEGORY_KEY___')
),

-- statistic accelerator
accelerator_statistic_pre AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, accelerator, 
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, accelerator
), 

accelerator_statistic AS (    
  SELECT 
    p.window_name, window_value, p.accelerator, 
    total_runs,
    passed_runs,
    runs_pass_rate,
    partial_passed_runs,
    runs_partial_pass_rate,    
    min_run_start, max_run_end,
    q.total_runs_qr
  FROM accelerator_statistic_pre p
  LEFT JOIN qr_accelerator_statistic q ON p.window_name = q.window_name AND 
    COALESCE(p.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.accelerator, '___NULL_CATEGORY_KEY___')
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

accelerator_tests_qr_qt_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, accelerator, 
    COUNT(test_id) AS total_run_tests_q,
    SUM(test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate_q,
  FROM filtered_tests_qr_qt
  GROUP BY window_name, accelerator
), 

accelerator_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, 
    r.partial_passed_runs, r.runs_partial_pass_rate, r.total_runs_qr,
    min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate,
    q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM accelerator_statistic r
  LEFT JOIN accelerator_tests_statistic t ON r.window_name = t.window_name 
    AND COALESCE(r.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(t.accelerator, '___NULL_CATEGORY_KEY___')
  LEFT JOIN accelerator_tests_qr_qt_statistic q ON r.window_name = q.window_name 
    AND COALESCE(r.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.accelerator, '___NULL_CATEGORY_KEY___')
),

tests_accelerator_statistic_pre AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, test_accelerator, 
    COUNT(test_id) AS total_run_tests,
    SUM(test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate,
  FROM filtered_tests
  GROUP BY window_name, test_accelerator
), 
tests_accelerator_qr_qt_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, test_accelerator, 
    COUNT(test_id) AS total_run_tests_q,
    SUM(test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate_q,
  FROM filtered_tests_qr_qt
  GROUP BY window_name, test_accelerator
), 

tests_accelerator_statistic AS (
  SELECT p.*, q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM tests_accelerator_statistic_pre p
  LEFT JOIN tests_accelerator_qr_qt_statistic q ON p.window_name = q.window_name 
    AND COALESCE(p.test_accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.test_accelerator, '___NULL_CATEGORY_KEY___') 
),

-- statistic category,accelerator
category_accelerator_statistic_pre AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, accelerator,
    COUNT(run_id) AS total_runs,
    SUM(is_passed) AS passed_runs,
    SAFE_DIVIDE(SUM(is_passed), COUNT(run_id)) AS runs_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_runs
  GROUP BY window_name, category, accelerator
), 
category_accelerator_statistic AS (    
  SELECT 
    p.window_name, window_value, p.category, p.accelerator, 
    total_runs,
    passed_runs,
    runs_pass_rate,
    partial_passed_runs,
    runs_partial_pass_rate,    
    min_run_start, max_run_end,
    q.total_runs_qr
  FROM category_accelerator_statistic_pre p
  LEFT JOIN qr_category_accelerator_statistic q ON p.window_name = q.window_name 
    AND COALESCE(p.category, '___NULL_CATEGORY_KEY___') = COALESCE(q.category, '___NULL_CATEGORY_KEY___') 
    AND COALESCE(p.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.accelerator, '___NULL_CATEGORY_KEY___')
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
category_accelerator_tests_qr_qt_statistic AS (    
  SELECT 
    window_name, ANY_VALUE(window_value) AS window_value, category, accelerator,
    COUNT(test_id) AS total_run_tests_q,
    SUM(test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(test_is_passed), COUNT(test_id)) AS tests_pass_rate_q,
  FROM filtered_tests_qr_qt
  GROUP BY window_name, category, accelerator
), 

category_accelerator_runs_tests_statistic AS (
  SELECT r.window_name, r.window_value, r.category, r.accelerator, r.total_runs, r.passed_runs, r.runs_pass_rate, 
    r.partial_passed_runs, r.runs_partial_pass_rate, r.total_runs_qr,
    min_run_start, max_run_end, t.total_run_tests, t.passed_tests, t.tests_pass_rate,
    q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM category_accelerator_statistic r
  LEFT JOIN category_accelerator_tests_statistic t ON r.window_name = t.window_name 
    AND COALESCE(r.category, '___NULL_CATEGORY_KEY___') = COALESCE(t.category, '___NULL_CATEGORY_KEY___')
    AND COALESCE(r.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(t.accelerator, '___NULL_CATEGORY_KEY___')  
  LEFT JOIN category_accelerator_tests_qr_qt_statistic q ON r.window_name = q.window_name 
    AND COALESCE(r.category, '___NULL_CATEGORY_KEY___') = COALESCE(q.category, '___NULL_CATEGORY_KEY___')
    AND COALESCE(r.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.accelerator, '___NULL_CATEGORY_KEY___')  
),

--------cluster 
dag_tests_cluster AS (
  SELECT dag_id, test_id, cluster_name, project_name, region, accelerator_type, accelerator_family, is_quarantined_dag, is_quarantined_test
  FROM `amy_xlml_poc_prod.cluster_info_view_latest`
),

-- DAG test statistic by cluster
dag_tests_statistic_cluster_pre AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, c.project_name, c.cluster_name, c.region,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name, c.cluster_name, c.region
), 
dag_tests_qr_qt_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, c.project_name, c.cluster_name, c.region,
    COUNT(t.test_id) AS total_run_tests_q,
    SUM(t.test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate_q
  FROM filtered_tests_qr_qt t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name, c.cluster_name, c.region
),
dag_tests_statistic_cluster AS (
  SELECT p.*, q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM dag_tests_statistic_cluster_pre p 
  LEFT JOIN dag_tests_qr_qt_statistic_cluster q 
    ON p.window_name = q.window_name AND p.project_name=q.project_name AND p.cluster_name=q.cluster_name AND p.region=q.region
),

-- DAG test category statistic by cluster
dag_tests_category_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, t.category, c.project_name, c.cluster_name, c.region,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.category, c.project_name, c.cluster_name, c.region
), 

-- DAG test accelerator statistic by cluster
dag_tests_accelerator_statistic_cluster AS (    
  SELECT 
    t.window_name, ANY_VALUE(window_value) AS window_value, t.accelerator, c.project_name, c.cluster_name, c.region,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, t.accelerator, c.project_name, c.cluster_name, c.region
), 

-- DAG test statistic by project
dag_tests_statistic_project_pre AS (    
  SELECT 
    t.window_name, ANY_VALUE(t.window_value) AS window_value,c.project_name,
    COUNT(t.test_id) AS total_run_tests,
    SUM(t.test_is_passed) AS passed_tests,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate,
    SUM(is_partial_passed) AS partial_passed_runs,
    SAFE_DIVIDE(SUM(is_partial_passed), COUNT(run_id)) AS runs_partial_pass_rate,    
    min(start_date) min_run_start, max(end_date) max_run_end
  FROM filtered_tests t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name
),
dag_tests_qr_qt_statistic_project AS (    
  SELECT 
    t.window_name, ANY_VALUE(t.window_value) AS window_value,c.project_name,
    COUNT(t.test_id) AS total_run_tests_q,
    SUM(t.test_is_passed) AS passed_tests_q,
    SAFE_DIVIDE(SUM(t.test_is_passed), COUNT(t.test_id)) AS tests_pass_rate_q
  FROM filtered_tests_qr_qt t
  JOIN dag_tests_cluster c ON t.dag_id = c.dag_id AND t.test_id = c.test_id
  GROUP BY t.window_name, c.project_name
),

dag_tests_statistic_project AS (  
  SELECT p.*, q.total_run_tests_q, q.passed_tests_q, q.tests_pass_rate_q
  FROM dag_tests_statistic_project_pre p 
  LEFT JOIN dag_tests_qr_qt_statistic_project q 
    ON p.window_name = q.window_name AND p.project_name=q.project_name
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
  cs.window_name, cs.window_value, cs.category, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, 
  cs.partial_passed_runs, cs.runs_partial_pass_rate, 
  cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end, cs.total_runs_qr,
  cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, 
  ARRAY(SELECT STRUCT(ds.accelerator, ds.dag_id, ds.total_runs, ds.passed_runs, ds.partial_passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, 
    formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name 
    AND COALESCE(ds.category, '___NULL_CATEGORY_KEY___') = COALESCE(cs.category, '___NULL_CATEGORY_KEY___')) AS dag_statistic
FROM category_runs_tests_statistic cs
),

dag_run_statistic_accelerator_array AS (
SELECT 
  cs.window_name, cs.window_value, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, cs.total_run_tests, 
  cs.partial_passed_runs, cs.runs_partial_pass_rate,
  cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end, cs.total_runs_qr,
  cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, 
  ARRAY(SELECT STRUCT(ds.category, ds.dag_id, ds.total_runs, ds.passed_runs, ds.partial_passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, 
    formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name 
    AND COALESCE(ds.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(cs.accelerator, '___NULL_CATEGORY_KEY___')) AS dag_statistic
FROM accelerator_runs_tests_statistic cs
),

dag_run_statistic_category_accelerator_array AS (
SELECT 
  cs.window_name, cs.window_value, cs.category, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, 
  cs.partial_passed_runs, cs.runs_partial_pass_rate,
  cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, min_run_start, max_run_end, cs.total_runs_qr,
  cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, 
  ARRAY(SELECT STRUCT(ds.dag_id, ds.total_runs, ds.passed_runs, ds.partial_passed_runs, ds.runs_pass_rate, ds.number_of_tests, ds.total_run_tests, ds.passed_tests, ds.tests_pass_rate, 
    formatted_schedule, is_paused, tags, total_runs_all, dag_owner, last_exec, last_succ) FROM dag_runs_tests_statistic ds WHERE ds.window_name=cs.window_name 
    AND COALESCE(ds.category, '___NULL_CATEGORY_KEY___') = COALESCE(cs.category, '___NULL_CATEGORY_KEY___')
    AND COALESCE(ds.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(cs.accelerator, '___NULL_CATEGORY_KEY___')) AS dag_statistic
FROM category_accelerator_runs_tests_statistic cs
),

t1_pre AS (
  SELECT d.window_name, ANY_VALUE(d.window_value) AS window_value,
  ARRAY_AGG(STRUCT(d.dag_id, d.category, d.accelerator, d.total_runs, d.passed_runs, d.runs_pass_rate, 
    d.partial_passed_runs, d.runs_partial_pass_rate,
    d.number_of_tests, min_run_start, max_run_end, d.total_run_tests, d.passed_tests, d.tests_pass_rate, 
    d.formatted_schedule, d.is_paused, d.tags, d.total_runs_all, d.dag_owner, d.last_exec, d.last_succ)) AS dag_statistic 
  FROM dag_runs_tests_statistic d
  GROUP by d.window_name
),

t1 AS (
  SELECT p.*, q.total_runs_qr
  FROM t1_pre p LEFT JOIN qr_statistic q ON p.window_name = q.window_name
),

t2 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.category, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, 
    cs.partial_passed_runs, cs.runs_partial_pass_rate, cs.total_runs_qr,
    min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, 
    cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, cs.dag_statistic)) AS dag_category_statistic
  FROM dag_run_statistic_category_array cs
  GROUP BY cs.window_name
),

t3 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, 
    cs.partial_passed_runs, cs.runs_partial_pass_rate, cs.total_runs_qr,
    min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, 
    cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, cs.dag_statistic)) AS dag_accelerator_statistic
  FROM dag_run_statistic_accelerator_array cs
  GROUP BY cs.window_name
),

t32 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
    ARRAY_AGG(STRUCT(cs.test_accelerator AS accelerator, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate, 
      cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q)) AS test_accelerator_statistic
  FROM tests_accelerator_statistic cs
  GROUP BY cs.window_name
),

t4 AS (
  SELECT cs.window_name, ANY_VALUE(cs.window_value) AS window_value,
  ARRAY_AGG(STRUCT(cs.category, cs.accelerator, cs.total_runs, cs.passed_runs, cs.runs_pass_rate, 
    cs.partial_passed_runs, cs.runs_partial_pass_rate, cs.total_runs_qr,
    min_run_start, max_run_end, cs.total_run_tests, cs.passed_tests, cs.tests_pass_rate,
    cs.total_run_tests_q, cs.passed_tests_q, cs.tests_pass_rate_q, cs.dag_statistic)) AS dag_category_accelerator_statistic
  FROM dag_run_statistic_category_accelerator_array cs
  GROUP BY cs.window_name
),

t5 AS (
  SELECT c.window_name, ANY_VALUE(c.window_value) AS window_value,
    ARRAY_AGG(STRUCT(c.project_name, c.cluster_name, c.region, min_run_start, max_run_end, c.total_run_tests, c.passed_tests, c.tests_pass_rate,
      c.total_run_tests_q, c.passed_tests_q, c.tests_pass_rate_q)) AS cluster_statistic
  FROM dag_tests_statistic_cluster c
  GROUP BY c.window_name
),

t6 AS (
  SELECT p.window_name, ANY_VALUE(p.window_value) AS window_value,
    ARRAY_AGG(STRUCT(p.project_name, min_run_start, max_run_end, p.total_run_tests, p.passed_tests, p.tests_pass_rate,
      p.total_run_tests_q, p.passed_tests_q, p.tests_pass_rate_q)) AS project_statistic
  FROM dag_tests_statistic_project p
  GROUP BY p.window_name
)

SELECT 
  d.window_name,    
  d.window_value,
  w.range_start_date,
  w.range_end_date,
  w.date_range_desc,
  d.dag_statistic,
  d.total_runs_qr,
  cat.dag_category_statistic,
  acc.dag_accelerator_statistic,
  ta.test_accelerator_statistic,
  ca.dag_category_accelerator_statistic,
  c.cluster_statistic,
  p.project_statistic
FROM  t1 d
LEFT JOIN t2 cat ON d.window_name = cat.window_name
LEFT JOIN t3 acc ON d.window_name = acc.window_name
LEFT JOIN t32 ta ON d.window_name = ta.window_name
LEFT JOIN t4 ca ON d.window_name = ca.window_name
LEFT JOIN t5 c ON d.window_name = c.window_name
LEFT JOIN t6 p ON d.window_name = p.window_name
LEFT JOIN windows_range_desc w ON d.window_name = w.window_name


