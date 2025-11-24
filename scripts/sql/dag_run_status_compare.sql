CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_run_status_compare` AS 
WITH 
filtered_runs_r AS (
  SELECT runs.run_order_desc AS run_th, base.category, base.accelerator, base.dag_id, base.tags, base.total_runs, base.total_tests,
    DATE(runs.start_date) AS run_date, runs.is_passed, runs.is_partial_passed
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs 
  WHERE runs.run_order_desc <= 4
),
filtered_runs_d AS (
  SELECT runs.run_order_desc AS run_th, base.category, base.accelerator, base.dag_id, base.tags, base.total_runs, base.total_tests,
    DATE(runs.start_date) AS run_date, runs.is_passed, runs.is_partial_passed
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  JOIN UNNEST (base.runs) AS runs
  WHERE DATE(start_date, 'UTC') BETWEEN DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 7 DAY) AND DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)
), 
filtered_daily AS (
  SELECT dag_id, MIN(run_th) AS run_th, ANY_VALUE(category) AS category, ANY_VALUE(accelerator) AS accelerator, ANY_VALUE(tags) AS tags, 
  ANY_VALUE(total_runs) AS total_runs, ANY_VALUE(total_tests) AS total_tests, run_date, 
  CASE
    WHEN COUNTIF(is_passed = 0 AND is_partial_passed = 0) > 0 THEN 0
    WHEN COUNTIF(is_partial_passed = 1) > 0 THEN 0
    ELSE 1
  END AS is_passed,
  CASE
    WHEN COUNTIF(is_partial_passed = 1) > 0 THEN 1
    ELSE 0
  END AS is_partial_passed,
  FROM filtered_runs_d
  GROUP BY dag_id, run_date
),
run_comparison_r AS (
  SELECT
    *, 'r' AS compare_type, concat('', run_th) AS current_th, concat('the_r_', run_th) AS current_window,
    LAG(is_passed, 1) OVER (PARTITION BY dag_id ORDER BY run_th DESC) AS prev_is_passed,
    LAG(is_partial_passed, 1) OVER (PARTITION BY dag_id ORDER BY run_th DESC) AS prev_is_partial_passed,
    concat('', LAG(run_th, 1) OVER (PARTITION BY dag_id ORDER BY run_th DESC)) AS prev_th, 
    concat('the_r_', LAG(run_th, 1) OVER (PARTITION BY dag_id ORDER BY run_th DESC)) AS prev_window
  FROM
    filtered_runs_r
),
run_comparison_d AS (
  SELECT *, 'd' AS compare_type, concat('',run_date) AS current_th,
    CONCAT('the_d_',
      CAST(DATE_DIFF(DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY), run_date, DAY) + 1 AS STRING)) AS current_window,
    LAG(is_passed, 1) OVER (PARTITION BY dag_id ORDER BY run_date ASC) AS prev_is_passed,
    LAG(is_partial_passed, 1) OVER (PARTITION BY dag_id ORDER BY run_date ASC) AS prev_is_partial_passed,
    concat('', LAG(run_date, 1) OVER (PARTITION BY dag_id ORDER BY run_date ASC)) AS prev_th,
    CONCAT('the_d_',
      CAST(DATE_DIFF(DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY), DATE(LAG(run_date, 1) OVER (PARTITION BY dag_id ORDER BY run_date ASC)), DAY) + 1 AS STRING)) AS prev_window,
  FROM
    filtered_runs_d
),

run_comparison AS (
  SELECT * FROM run_comparison_r
  UNION ALL
  SELECT * FROM run_comparison_d
),

state_labeling AS (
  SELECT
    dag_id,
    category,
    accelerator,
    tags,
    compare_type,
    current_th,
    prev_th,
    concat(prev_th," TO ",current_th) AS compare_pair,    
    current_window,
    prev_window,
    CASE
      WHEN is_passed = 1 THEN 'PASSED'
      WHEN is_partial_passed = 1 THEN 'PARTIAL'
      ELSE 'FAILED'
    END AS current_state,
    CASE
      WHEN prev_is_passed = 1 THEN 'PASSED'
      WHEN prev_is_partial_passed = 1 THEN 'PARTIAL'
      ELSE 'FAILED'
    END AS prev_state
    FROM run_comparison
),

transition_labeling AS (
  SELECT
    dag_id,
    category,
    accelerator,
    tags,
    compare_type,
    current_th,
    prev_th,
    compare_pair,
    current_window,
    prev_window,
    current_state,
    prev_state,
    CASE
      WHEN prev_state = 'FAILED' AND current_state='PARTIAL' THEN 'FAILED_TO_PARTIAL'
      WHEN prev_state = 'FAILED' AND current_state='PASSED' THEN 'FAILED_TO_PASSED'
      WHEN prev_state = 'PARTIAL' AND current_state='PASSED' THEN 'PARTIAL_TO_PASSED'
      WHEN prev_state = 'PARTIAL' AND current_state='FAILED' THEN 'PARTIAL_TO_FAILED' 
      WHEN prev_state = 'PASSED' AND current_state='FAILED' THEN 'PASSED_TO_FAILED'
      WHEN prev_state = 'PASSED' AND current_state='PARTIAL' THEN 'PASSED_TO_PARTIAL'
      ELSE 'NO_CHANGE'
    END AS transition_type
    FROM state_labeling
)

SELECT
  dag_id,
  --category,
  --accelerator,
  --tags,
  compare_type, 
  current_th,
  prev_th,
  compare_pair,
  current_window,
  prev_window,
  current_state,
  prev_state,
  transition_type
FROM
  transition_labeling
WHERE
  prev_th IS NOT NULL AND transition_type != 'NO_CHANGE' 
ORDER BY
  compare_type, compare_pair, transition_type, dag_id
  
