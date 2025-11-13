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
windows_wu_the AS (
  SELECT value,CONCAT('the_wu_', value) AS window_name FROM `amy_xlml_poc_prod.config_window` WHERE type='the_wu'
),
date_range_calc_d AS (
  SELECT
    w.window_name,
    w.value AS window_value,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AS range_start_date,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY) AS range_end_date,
    CASE
      WHEN DATE_DIFF(DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY), DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY), DAY) = 0 
        THEN FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY))
      ELSE FORMAT(
        '%s to %s',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)),
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY))
      )
    END AS date_range_desc
  FROM
    windows_d AS w
),
date_range_calc_d_the AS (
  SELECT
    w.window_name,
    w.value AS window_value,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AS range_start_date,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AS range_end_date,
    FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)) AS date_range_desc
  FROM
    windows_d_the AS w
),
w_the AS (
  SELECT * FROM windows_w_the
  UNION ALL
  SELECT * FROM windows_wu_the
),

date_range_calc_w_the AS (
  SELECT
    w.window_name,
    w.value AS window_value,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY) AS range_start_date,
    DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY) AS range_end_date,
    FORMAT(
        '%s to %s',
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)),
        FORMAT_DATE('%Y-%m-%d', DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY))
      )
    AS date_range_desc
  FROM
    w_the AS w
),
windows_range_desc AS (
  SELECT window_name,range_start_date,range_end_date,date_range_desc FROM date_range_calc_d UNION ALL  
  SELECT window_name,range_start_date,range_end_date,date_range_desc FROM date_range_calc_d_the UNION ALL
  SELECT window_name,range_start_date,range_end_date,date_range_desc FROM date_range_calc_w_the 
),


all_dags AS (
  SELECT dag_id, dag_owners, formatted_schedule, is_paused, tags, category, accelerator, 
    total_runs AS total_runs_all, total_runs_iq, passed_runs, partial_passed_runs, failed_runs, quarantined_runs, 
    total_tests, total_tests_qe, total_tests_q, test_ids, test_ids_qe, test_ids_q,
    last_exec, last_succ
  FROM `cienet-cmcs.amy_xlml_poc_prod.base`
),

-- Filter last N UTC days
filtered_runs_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc,
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  --WHERE start_date BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY) AND CURRENT_TIMESTAMP()
  JOIN windows_d w ON DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
  --JOIN windows_d w ON DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND CURRENT_DATE('UTC')
), 

filtered_runs_qr_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  JOIN windows_d w ON DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
), 

filtered_tests_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'normal' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_d w ON DATE(runs.start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
),

filtered_tests_qr_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qr' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_d w ON DATE(runs.start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
  WHERE base.total_tests = base.total_tests_q  
),
filtered_tests_qt_d AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qt' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_d w ON DATE(runs.start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
  WHERE base.total_tests_q > 0  
),

-- Filter last N runs
filtered_runs_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_r w ON runs.run_order_desc <= w.value
), 

filtered_runs_qr_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  JOIN windows_r w ON runs.run_order_desc <= w.value
), 

filtered_tests_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'normal' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_r w ON runs.run_order_desc <= w.value
), 
filtered_tests_qr_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qr' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_r w ON runs.run_order_desc <= w.value
  WHERE base.total_tests = base.total_tests_q    
), 
filtered_tests_qt_r AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qt' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_r w ON runs.run_order_desc <= w.value
  WHERE base.total_tests_q > 0 
), 

-- Filter the Nth run
filtered_runs_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_r_the w ON runs.run_order_desc = w.value
), 

filtered_runs_qr_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  JOIN windows_r_the w ON runs.run_order_desc = w.value
), 

filtered_tests_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'normal' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_r_the w ON runs.run_order_desc = w.value
), 
filtered_tests_qr_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qr' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_r_the w ON runs.run_order_desc = w.value
  WHERE base.total_tests = base.total_tests_q    
), 
filtered_tests_qt_r_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qt' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_r_the w ON runs.run_order_desc = w.value
  WHERE base.total_tests_q > 0 
), 

-- Filter the Nth day
filtered_runs_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
), 

filtered_runs_qr_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
), 

filtered_tests_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'normal' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
), 
filtered_tests_qr_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qr' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
  WHERE base.total_tests = base.total_tests_q    
), 
filtered_tests_qt_d_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, 
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qt' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_d_the w ON DATE(runs.start_date, 'UTC') = DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value DAY)
  WHERE base.total_tests_q > 0 
), 

-- Filter the Nth week
filtered_runs_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc 
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
), 

filtered_runs_qr_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
), 

filtered_tests_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'normal' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests) AS tests
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
), 
filtered_tests_qr_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qr' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
  WHERE base.total_tests = base.total_tests_q 
),  
filtered_tests_qt_w_the AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,  
    tests.test_id,tests.is_passed test_is_passed,tests.start_date test_start_date, tests.end_date test_end_date, tests.accelerator AS test_accelerator,
    'qt' AS type
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  LEFT JOIN UNNEST (runs.tests_q) AS tests
  JOIN windows_w_the w ON DATE(runs.start_date, 'UTC')
    BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
    AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
  WHERE base.total_tests_q > 0     
), 

-- Filter the Nth week with DAG uniqueness
filtered_runs_wu_the AS (
WITH PrioritizedRuns AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc, runs.is_passed_run_order_desc,
    -- Rank the runs for each dag_id
    ROW_NUMBER() OVER (
      PARTITION BY window_name,window_name,base.dag_id
      ORDER BY
        runs.is_passed DESC,         -- Priority 1: True (1) > False (0)
        runs.is_partial_passed DESC  -- Priority 2: True (1) > False (0)
    ) AS run_rank
  FROM
    `cienet-cmcs.amy_xlml_poc_prod.base` AS base
  LEFT JOIN
    UNNEST (base.runs) AS runs
  JOIN windows_wu_the AS w ON DATE(runs.start_date, 'UTC')
      BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
      AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
)
SELECT window_name, window_value, run_th, category, accelerator, dag_id, total_runs, total_tests,
    run_id, execution_date, start_date, end_date, is_passed, is_partial_passed, run_order_desc, is_passed_run_order_desc
FROM PrioritizedRuns
WHERE run_rank = 1
),    
filtered_runs_qr_wu_the AS (
WITH PrioritizedRuns AS (
  SELECT w.window_name, w.value window_value, runs.run_order_desc run_th, base.category, base.accelerator, base.dag_id, base.total_runs, base.total_tests,
    runs.run_id, runs.execution_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed, runs.run_order_desc,
    -- Rank the runs for each dag_id
    ROW_NUMBER() OVER (
      PARTITION BY window_name,window_name,base.dag_id
      ORDER BY
        runs.is_passed DESC,         -- Priority 1: True (1) > False (0)
        runs.is_partial_passed DESC  -- Priority 2: True (1) > False (0)
    ) AS run_rank
  FROM
    `cienet-cmcs.amy_xlml_poc_prod.base` AS base
  LEFT JOIN
    UNNEST (base.runs_qr) AS runs
  JOIN windows_wu_the AS w ON DATE(runs.start_date, 'UTC')
      BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL w.value * 7 DAY)
      AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL (w.value * 7 - 6) DAY)
)
SELECT window_name, window_value, run_th, category, accelerator, dag_id, total_runs, total_tests,
    run_id, execution_date, start_date, end_date, is_passed, is_partial_passed, run_order_desc
FROM PrioritizedRuns
WHERE run_rank = 1
),    

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
  UNION ALL
  SELECT * from filtered_runs_wu_the  
),

filtered_runs_qr AS (
  SELECT * from filtered_runs_qr_d
  UNION ALL
  SELECT * from filtered_runs_qr_r
  UNION ALL
  SELECT * from filtered_runs_qr_r_the
  UNION ALL
  SELECT * from filtered_runs_qr_d_the
  UNION ALL
  SELECT * from filtered_runs_qr_w_the  
  UNION ALL
  SELECT * from filtered_runs_qr_wu_the  
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

filtered_tests_qr_qt AS (
  SELECT * from filtered_tests_qr_d
  UNION ALL
  SELECT * from filtered_tests_qr_r
  UNION ALL
  SELECT * from filtered_tests_qr_r_the
  UNION ALL
  SELECT * from filtered_tests_qr_d_the
  UNION ALL
  SELECT * from filtered_tests_qr_w_the
  UNION ALL
  SELECT * from filtered_tests_qt_d
  UNION ALL
  SELECT * from filtered_tests_qt_r
  UNION ALL
  SELECT * from filtered_tests_qt_r_the
  UNION ALL
  SELECT * from filtered_tests_qt_d_the
  UNION ALL
  SELECT * from filtered_tests_qt_w_the    
)

SELECT
  -- 1. all_dags (Static, window-independent data)
  (
    SELECT ARRAY_AGG(STRUCT(
      t1.dag_id, t1.dag_owners, t1.formatted_schedule, t1.is_paused, t1.tags, t1.category, t1.accelerator, 
      t1.total_runs_all, t1.total_runs_iq, t1.passed_runs, t1.partial_passed_runs, t1.failed_runs, t1.quarantined_runs, 
      t1.total_tests, t1.total_tests_qe, t1.total_tests_q, t1.test_ids, t1.test_ids_qe, t1.test_ids_q,
      t1.last_exec, t1.last_succ
    ))
    FROM all_dags AS t1
  ) AS all_dags,
  
  -- 2. windows_range_desc (Window metadata)
  (
    SELECT ARRAY_AGG(STRUCT(
      t2.window_name,
      t2.range_start_date,
      t2.range_end_date,
      t2.date_range_desc
    ))
    FROM windows_range_desc AS t2
  ) AS windows_range_desc,
  
  -- 3. filtered_runs (Filtered runs data)
  (
    SELECT ARRAY_AGG(STRUCT(
      t3.window_name, t3.window_value, t3.run_th, t3.category, t3.accelerator, t3.dag_id, t3.total_runs, t3.total_tests,
      t3.run_id, t3.execution_date, t3.start_date, t3.end_date, t3.is_passed, t3.is_partial_passed, t3.run_order_desc, t3.is_passed_run_order_desc
    ))
    FROM filtered_runs AS t3
  ) AS filtered_runs,
  
  -- 4. filtered_runs_qr (Filtered quarantined runs data)
  (
    SELECT ARRAY_AGG(STRUCT(
      t4.window_name, t4.window_value, t4.run_th, t4.category, t4.accelerator, t4.dag_id, t4.total_runs, t4.total_tests,
      t4.run_id, t4.execution_date, t4.start_date, t4.end_date, t4.is_passed, t4.is_partial_passed, t4.run_order_desc
    ))
    FROM filtered_runs_qr AS t4
  ) AS filtered_runs_qr,
  
  -- 5. filtered_tests (Filtered tests data)
  (
    SELECT ARRAY_AGG(STRUCT(
      t5.window_name, t5.window_value, t5.run_th, t5.category, t5.accelerator, t5.dag_id, t5.total_runs, t5.total_tests,
      t5.run_id, t5.execution_date, t5.start_date, t5.end_date, t5.is_passed, t5.is_partial_passed, t5.run_order_desc, 
      t5.test_id, t5.test_is_passed, t5.test_start_date, t5.test_end_date, t5.test_accelerator, t5.type
    ))
    FROM filtered_tests AS t5
  ) AS filtered_tests,

-- 6. filtered_tests_qr_qt
  (
    SELECT ARRAY_AGG(STRUCT(
      t5.window_name, t5.window_value, t5.run_th, t5.category, t5.accelerator, t5.dag_id, t5.total_runs, t5.total_tests,
      t5.run_id, t5.execution_date, t5.start_date, t5.end_date, t5.is_passed, t5.is_partial_passed, t5.run_order_desc, 
      t5.test_id, t5.test_is_passed, t5.test_start_date, t5.test_end_date, t5.test_accelerator, t5.type
    ))
    FROM filtered_tests_qr_qt AS t5
  ) AS filtered_tests_qr_qt

