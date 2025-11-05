CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_status_base` AS
WITH
full_dag_runs_stat AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_stat' AS data_type, 
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests, runs.successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
),
full_dag_runs_scheduled AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_scheduled' AS data_type,
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests, runs.successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_scheduled) AS runs
),
full_dag_runs_manual AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_manual' AS data_type,
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests, runs.successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_manual) AS runs
),
full_dag_runs_qr AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_quarantined' AS data_type,
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests_q AS total_tests, runs.successful_tests_q AS successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs_qr) AS runs
),
full_dag_runs_qt AS (
  SELECT base.category, base.accelerator, base.dag_owners dag_owner, base.tags, base.formatted_schedule, base.is_paused, base.dag_id, base.total_runs, 
    base.total_tests AS base_total_tests,
    CASE WHEN base.total_tests != base.total_tests_qe THEN TRUE ELSE FALSE END AS is_quarantined,
    CASE WHEN base.total_tests = base.total_tests_q THEN TRUE ELSE FALSE END AS is_quarantined_dag,    
    'type_quarantined' AS data_type,
    runs.run_id, runs.execution_date, DATE(runs.start_date) AS run_date, runs.start_date, runs.end_date, runs.is_passed, runs.is_partial_passed,
    runs.total_tests_q AS total_tests, runs.successful_tests_q AS successful_tests
  FROM `cienet-cmcs.amy_xlml_poc_prod.base` base
  LEFT JOIN UNNEST (base.runs) AS runs
  WHERE runs.total_tests_q > 0
),


full_dag_runs AS (
  SELECT * FROM full_dag_runs_stat
  UNION ALL
  SELECT * FROM full_dag_runs_scheduled
  UNION ALL
  SELECT * FROM full_dag_runs_manual
  UNION ALL
  SELECT * FROM full_dag_runs_qr
  UNION ALL
  SELECT * FROM full_dag_runs_qt
),

daily_dag_status AS (
  SELECT
    fdr.dag_id,
    fdr.data_type,
    ANY_VALUE(category) AS category,
    ANY_VALUE(accelerator) AS accelerator,
    ANY_VALUE(fdr.dag_owner) AS dag_owner,
    ANY_VALUE(fdr.tags) AS tags,
    ANY_VALUE(fdr.formatted_schedule) AS formatted_schedule,
    ANY_VALUE(fdr.is_paused) AS is_paused,
    fdr.run_date AS date,
    CASE
      WHEN COUNT(fdr.run_id) = 0 THEN 'no run'
      --WHEN COUNTIF(fdr.is_passed = 0) > 0 THEN 'failed'
      WHEN COUNTIF(fdr.is_passed = 0 AND fdr.is_partial_passed = 0) > 0 THEN 'failed'
      WHEN COUNTIF(fdr.is_partial_passed = 1) > 0 THEN 'partial'
      ELSE 'success'
    END AS status,
    ANY_VALUE(fdr.base_total_tests) AS total_tests_all,
    ANY_VALUE(is_quarantined) AS is_quarantined,
    ANY_VALUE(is_quarantined_dag) AS is_quarantined_dag,
    SUM(fdr.total_tests) AS total_tests,
    SUM(fdr.successful_tests) AS successful_tests,
    SUM(fdr.total_tests) - SUM(fdr.successful_tests) AS failed_tests
  FROM
    full_dag_runs AS fdr
  GROUP BY
    fdr.dag_id,
    fdr.run_date,
    fdr.data_type
),

pivoted_daily_status AS (
  SELECT
    dag_id,
    date,
    -- Pivot the 'type_stat' metrics for the original daily_status
    ANY_VALUE(CASE WHEN data_type = 'type_stat' THEN status END) AS status_stat,
    ANY_VALUE(CASE WHEN data_type = 'type_stat' THEN total_tests END) AS total_tests_stat,
    ANY_VALUE(CASE WHEN data_type = 'type_stat' THEN successful_tests END) AS successful_tests_stat,
    ANY_VALUE(CASE WHEN data_type = 'type_stat' THEN failed_tests END) AS failed_tests_stat,
    
    -- Pivot the 'type_scheduled' metrics
    ANY_VALUE(CASE WHEN data_type = 'type_scheduled' THEN status END) AS status_scheduled,
    ANY_VALUE(CASE WHEN data_type = 'type_scheduled' THEN total_tests END) AS total_tests_scheduled,
    ANY_VALUE(CASE WHEN data_type = 'type_scheduled' THEN successful_tests END) AS successful_tests_scheduled,
    ANY_VALUE(CASE WHEN data_type = 'type_scheduled' THEN failed_tests END) AS failed_tests_scheduled,
    
    -- Pivot the 'type_manual' metrics
    ANY_VALUE(CASE WHEN data_type = 'type_manual' THEN status END) AS status_manual,
    ANY_VALUE(CASE WHEN data_type = 'type_manual' THEN total_tests END) AS total_tests_manual,
    ANY_VALUE(CASE WHEN data_type = 'type_manual' THEN successful_tests END) AS successful_tests_manual,
    ANY_VALUE(CASE WHEN data_type = 'type_manual' THEN failed_tests END) AS failed_tests_manual,

    -- Pivot the 'type_quarantined' metrics
    ANY_VALUE(CASE WHEN data_type = 'type_quarantined' THEN status END) AS status_quarantined,
    ANY_VALUE(CASE WHEN data_type = 'type_quarantined' THEN total_tests END) AS total_tests_quarantined,
    ANY_VALUE(CASE WHEN data_type = 'type_quarantined' THEN successful_tests END) AS successful_tests_quarantined,
    ANY_VALUE(CASE WHEN data_type = 'type_quarantined' THEN failed_tests END) AS failed_tests_quarantined

  FROM daily_dag_status
  GROUP BY 1, 2
),

distinct_dag_details AS (
  SELECT DISTINCT
    dag_id,
    dag_owner,
    tags,
    category,
    accelerator,
    formatted_schedule,
    is_paused,
    total_tests_all,
    is_quarantined,
    is_quarantined_dag
  FROM
    daily_dag_status
),
min_max_dates AS (
  SELECT
    MIN(run_date) AS min_date,
    MAX(run_date) AS max_date
  FROM
    full_dag_runs
),
date_series AS (
  SELECT
    CAST(d AS DATE) AS date
  FROM
    UNNEST(GENERATE_DATE_ARRAY((SELECT min_date FROM min_max_dates), (SELECT max_date FROM min_max_dates))) AS d
)

SELECT
  ddd.dag_id,
  ddd.dag_owner,
  ddd.tags,
  ddd.category,
  ddd.accelerator,
  ddd.is_paused,
  ddd.formatted_schedule,
  ddd.total_tests_all,
  ddd.is_quarantined,
  ddd.is_quarantined_dag,
  ds.date,
  --STRUCT(
  --  COALESCE(dds.status, 'no run') AS status,
  --  ds.total_tests AS total_tests,
  --  dds.successful_tests AS successful_tests,
  --  dds.failed_tests AS failed_tests
  --) AS daily_status
  STRUCT(
    COALESCE(pds.status_stat, 'no run') AS status,
    pds.total_tests_stat AS total_tests,
    pds.successful_tests_stat AS successful_tests,
    pds.failed_tests_stat AS failed_tests
  ) AS daily_status,
  STRUCT(
    COALESCE(pds.status_scheduled, 'no run') AS status,
    pds.total_tests_scheduled AS total_tests,
    pds.successful_tests_scheduled AS successful_tests,
    pds.failed_tests_scheduled AS failed_tests
  ) AS daily_status_scheduled,
  STRUCT(
    COALESCE(pds.status_manual, 'no run') AS status,
    pds.total_tests_manual AS total_tests,
    pds.successful_tests_manual AS successful_tests,
    pds.failed_tests_manual AS failed_tests
  ) AS daily_status_manual,
  STRUCT(
    COALESCE(pds.status_quarantined, 'no run') AS status,
    pds.total_tests_quarantined AS total_tests,
    pds.successful_tests_quarantined AS successful_tests,
    pds.failed_tests_quarantined AS failed_tests
  ) AS daily_status_quarantined,      
FROM
  distinct_dag_details AS ddd
CROSS JOIN
  date_series AS ds
LEFT JOIN
  pivoted_daily_status AS pds ON ddd.dag_id = pds.dag_id AND ds.date = pds.date  
--LEFT JOIN
--  daily_dag_status AS dds ON ddd.dag_id = dds.dag_id AND ds.date = dds.date
  
