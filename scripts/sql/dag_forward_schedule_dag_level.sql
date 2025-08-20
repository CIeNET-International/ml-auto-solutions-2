CREATE OR REPLACE VIEW `amy_xlml_poc_2.dag_forward_schedule_dag_level` AS
WITH dag_agg AS (
  SELECT
    cluster_name,
    dag_id,
    formatted_schedule,
    DATE(test_sim_start_time) AS sim_date,
    MIN(test_sim_start_time) AS cluster_sim_start,
    MAX(test_sim_end_time)   AS cluster_sim_end,
    COUNT(*) AS cluster_tests,
    ARRAY_AGG(
      STRUCT(
        test_id,
        test_sim_start_time,
        test_sim_end_time
      )
      ORDER BY test_sim_start_time
    ) AS test_details
  FROM `amy_xlml_poc_2.dashboard_dag_forward_schedule_simulation`
  GROUP BY cluster_name, dag_id, formatted_schedule, sim_date
),
concurrency AS (
  SELECT
    t.cluster_name,
    t.dag_id,
    t.formatted_schedule,
    DATE(t.test_sim_start_time) AS sim_date,
    MAX((
      SELECT COUNT(1)
      FROM `amy_xlml_poc_2.dashboard_dag_forward_schedule_simulation` s
      WHERE s.cluster_name = t.cluster_name
        AND s.dag_id = t.dag_id
        AND s.formatted_schedule = t.formatted_schedule
        AND DATE(s.test_sim_start_time) = DATE(t.test_sim_start_time)
        AND s.test_sim_start_time < t.test_sim_end_time
        AND s.test_sim_end_time   > t.test_sim_start_time
    )) AS max_concurrent_tests
  FROM `amy_xlml_poc_2.dashboard_dag_forward_schedule_simulation` t
  GROUP BY t.cluster_name, t.dag_id, t.formatted_schedule, sim_date
)
SELECT
  a.cluster_name,
  a.dag_id,
  a.formatted_schedule,
  a.sim_date,
  -- keep original dag_sim_start/dag_sim_end (representative daily values)
  ANY_VALUE(t.dag_sim_start) AS dag_sim_start,
  ANY_VALUE(t.dag_sim_end)   AS dag_sim_end,
  -- aggregated daily bounds
  a.cluster_sim_start,
  a.cluster_sim_end,
  a.cluster_tests,
  a.test_details,
  c.max_concurrent_tests
FROM dag_agg a
JOIN concurrency c
  ON a.cluster_name = c.cluster_name
 AND a.dag_id = c.dag_id
 AND a.formatted_schedule = c.formatted_schedule
 AND a.sim_date = c.sim_date
JOIN `amy_xlml_poc_2.dashboard_dag_forward_schedule_simulation` t
  ON t.cluster_name = a.cluster_name
 AND t.dag_id = a.dag_id
 AND t.formatted_schedule = a.formatted_schedule
 AND DATE(t.test_sim_start_time) = a.sim_date
GROUP BY
  a.cluster_name,
  a.dag_id,
  a.formatted_schedule,
  a.sim_date,
  a.cluster_sim_start,
  a.cluster_sim_end,
  a.cluster_tests,
  a.test_details,
  c.max_concurrent_tests
ORDER BY cluster_name, dag_id, sim_date;

