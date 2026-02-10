CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.dag_run_metric_view` AS
WITH tpu_obs_tesets AS (
  SELECT 
    v.dag_id,
    t.test_id,
    t.test_start_date,
    t.test_end_date,
    t.test_status,
    t.cluster_name,
    t.cluster_project,
    t.node_pool_name,
    t.cluster_region,
    t.accelerator_type,
    t.accelerator_family,
    run_id,execution_date,
    run_start_date,
    run_end_date,
    run_status
  FROM `cienet-cmcs.amy_xlml_poc_prod.dag_runs_with_logs_view` v,
  UNNEST(v.tests) t
  WHERE t.node_pool_name IS NOT NULL
),
metric_mapping AS (
  SELECT 
    m.dag_id,
    m.test_id,
    m.is_active,
    temp.metric_disp_name,
    temp.page_state_template
  FROM `cienet-cmcs.amy_xlml_poc_prod.config_test_metric` m
  INNER JOIN `cienet-cmcs.amy_xlml_poc_prod.config_metric_template` temp 
    ON m.metric_name = temp.metric_name
  WHERE m.is_active = TRUE
)
SELECT
  e.test_id,
  e.dag_id,
  e.test_start_date,
  e.test_end_date,
  e.test_status,
  e.cluster_name,
  e.cluster_project,
  e.node_pool_name,
  e.cluster_region,
  e.accelerator_type,
  e.accelerator_family,
  e.run_id,
  e.execution_date,
  e.run_start_date,
  e.run_end_date,
  e.run_status,  
  mm.metric_disp_name,
  CONCAT(
    'https://console.cloud.google.com/monitoring/metrics-explorer?',
    'project=', e.cluster_project,
    '&startTime=', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', e.test_start_date),
    '&endTime=', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', e.test_end_date),
    '&pageState=',
    -- Step 3: Inject runtime variables into the page_state_template
    REPLACE(REPLACE(REPLACE(
      mm.page_state_template,
      '{{CLUSTER}}', e.cluster_name),
      '{{POOL}}', e.node_pool_name),
      '{{PROJECT}}', e.cluster_project)
  ) AS monitoring_url
FROM tpu_obs_tesets e
INNER JOIN metric_mapping mm 
  ON e.dag_id = mm.dag_id AND e.test_id = mm.test_id

