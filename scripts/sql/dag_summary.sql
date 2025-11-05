CREATE OR REPLACE VIEW `amy_xlml_poc_prod.dag_summary` AS
WITH 
all_dags AS (
  SELECT * FROM `amy_xlml_poc_prod.base` 
),

all_category_accelerator AS (
  SELECT category, accelerator, COUNT(DISTINCT dag_id) AS total_dags
  FROM all_dags
  GROUP BY category, accelerator
),

norun AS (
  SELECT category, accelerator, COUNT(DISTINCT dag_id) AS dags_norun
  FROM all_dags
  WHERE total_runs_iq = 0 OR total_runs_iq is null
  GROUP BY category, accelerator
),

qr AS (
  SELECT category, accelerator, COUNT(DISTINCT dag_id) AS dags_quarantined
  FROM all_dags
  WHERE quarantined_runs > 0
  GROUP BY category, accelerator
),

paused AS (
  SELECT category, accelerator, COUNT(DISTINCT dag_id) AS dags_paused
  FROM all_dags
  WHERE is_paused = TRUE
  GROUP BY category, accelerator
),

unsch AS (
  SELECT category, accelerator, COUNT(DISTINCT dag_id) AS dags_unscheduled
  FROM all_dags
  WHERE schedule_interval is NULL OR schedule_interval = '' OR LOWER(schedule_interval) = 'null'
  GROUP BY category, accelerator
),

statistic_pre AS (
  SELECT
    s.category, 
    s.accelerator,
    d.dag_id, 
    d.total_runs,
    d.passed_runs,
    d.partial_passed_runs,
    d.total_runs - d.passed_runs - d.partial_passed_runs AS failed_runs,
    CASE
      WHEN d.total_runs = d.passed_runs THEN 'success'
      WHEN d.partial_passed_runs > 0 THEN 'partial'
      ELSE 'failed'
    END AS status,    
  FROM 
    `amy_xlml_poc_prod.statistic_last_window` as w,
    UNNEST(w.dag_category_accelerator_statistic) as s,
    UNNEST(s.dag_statistic) d
  WHERE window_name like 'last_3r'
),

statistic AS (
  SELECT 
    category, 
    accelerator, 
    SUM(CASE WHEN status = 'success'THEN 1 ELSE 0 END) AS dags_passed,
    SUM(CASE WHEN status = 'partial'THEN 1 ELSE 0 END) AS dags_partial_passed,
    SUM(CASE WHEN status = 'failed'THEN 1 ELSE 0 END) AS dags_failed,
    COUNT(DISTINCT dag_id) AS total_dags,
    SAFE_DIVIDE(SUM(CASE WHEN status = 'success'THEN 1 ELSE 0 END), COUNT(DISTINCT dag_id)) AS pass_rate,    
  FROM statistic_pre
  GROUP BY category, accelerator
)

SELECT 
  d.category, 
  d.accelerator, 
  d.total_dags, 
  s.total_dags AS total_run_dags,
  s.pass_rate,
  s.dags_passed,
  s.dags_partial_passed,
  s.dags_failed,
  n.dags_norun, 
  q.dags_quarantined, 
  p.dags_paused, 
  u.dags_unscheduled,
FROM all_category_accelerator d
LEFT JOIN norun n ON 
  COALESCE(d.category, '___NULL_CATEGORY_KEY___') = COALESCE(n.category, '___NULL_CATEGORY_KEY___')
  AND COALESCE(d.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(n.accelerator, '___NULL_CATEGORY_KEY___')
LEFT JOIN qr q ON 
  COALESCE(d.category, '___NULL_CATEGORY_KEY___') = COALESCE(q.category, '___NULL_CATEGORY_KEY___')
  AND COALESCE(d.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(q.accelerator, '___NULL_CATEGORY_KEY___')
LEFT JOIN paused p ON
  COALESCE(d.category, '___NULL_CATEGORY_KEY___') = COALESCE(p.category, '___NULL_CATEGORY_KEY___')
  AND COALESCE(d.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(p.accelerator, '___NULL_CATEGORY_KEY___')
LEFT JOIN unsch u ON
  COALESCE(d.category, '___NULL_CATEGORY_KEY___') = COALESCE(u.category, '___NULL_CATEGORY_KEY___')
  AND COALESCE(d.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(u.accelerator, '___NULL_CATEGORY_KEY___')
LEFT JOIN statistic s ON
  COALESCE(d.category, '___NULL_CATEGORY_KEY___') = COALESCE(s.category, '___NULL_CATEGORY_KEY___')
  AND COALESCE(d.accelerator, '___NULL_CATEGORY_KEY___') = COALESCE(s.accelerator, '___NULL_CATEGORY_KEY___')





