CREATE OR REPLACE VIEW `amy_xlml_poc_2.dag_forward_schedule_simulation` AS
WITH
-- Fixed 00:00 UTC "today" and exclusive end at +7 days
day0 AS (
  SELECT
    TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, "UTC") AS sim_day0,
    TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, "UTC"), INTERVAL 7 DAY) AS sim_day7
),

dag_sch AS (
  SELECT
    dag_id,
    schedule_interval,
    CASE
      WHEN STARTS_WITH(schedule_interval, '{') AND JSON_VALUE(schedule_interval, '$.type') = 'timedelta' THEN
        -- Logic for timedelta schedules
        CONCAT(
          CAST(JSON_VALUE(schedule_interval, '$.attrs.days') AS INT64), ' day, ',
          LPAD(CAST(FLOOR(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64) / 3600) AS STRING), 2, '0'),
          ':',
          LPAD(CAST(FLOOR(MOD(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64), 3600) / 60) AS STRING), 2, '0'),
          ':',
          LPAD(CAST(MOD(CAST(JSON_VALUE(schedule_interval, '$.attrs.seconds') AS INT64), 60) AS STRING), 2, '0')
        )
      ELSE
        -- Logic for cron expressions and other string-based schedules
        schedule_interval
    END AS formatted_schedule
  FROM
    `amy_xlml_poc_2.dag` 
  WHERE schedule_interval IS NOT NULL
    AND LOWER(schedule_interval) != 'null'
    AND is_paused = FALSE
),
    
-- Per-test profile + schedule + DAG avg duration
profiles AS (
  SELECT
    p.cluster_name,
    p.dag_id,
    p.test_id,
    p.start_offset_seconds,
    COALESCE(p.avg_successful_test_duration_seconds, p.avg_any_test_duration_seconds) AS test_duration_seconds,
    g.schedule_interval,
    COALESCE(ds.avg_duration_success_seconds, ds.avg_duration_any_seconds) AS dag_duration_seconds
  FROM `amy_xlml_poc_2.dag_execution_profile_with_cluster` p
  JOIN dag_sch g
    ON p.dag_id = g.dag_id
  JOIN `amy_xlml_poc_2.dag_duration_stat` ds
    ON p.dag_id = ds.dag_id
  WHERE p.cluster_name IS NOT NULL
    AND TRIM(p.cluster_name) != ''  -- skip unmapped tests
),

-- Expand DAG run starts across the next 7 days
expanded AS (
  SELECT
    pr.*,
    run_ts AS dag_run_start
  FROM profiles pr
  CROSS JOIN day0 d
  CROSS JOIN UNNEST(
    (
      CASE
        WHEN pr.schedule_interval = '@daily' THEN
          GENERATE_TIMESTAMP_ARRAY(d.sim_day0, d.sim_day7, INTERVAL 1 DAY)
        WHEN pr.schedule_interval = '@hourly' THEN
          GENERATE_TIMESTAMP_ARRAY(d.sim_day0, d.sim_day7, INTERVAL 1 HOUR)
        WHEN REGEXP_CONTAINS(pr.schedule_interval, r'^\s*\d+\s+\d+\s+\*\s+\*\s+\*\s*$') THEN
          GENERATE_TIMESTAMP_ARRAY(
            TIMESTAMP_ADD(
              TIMESTAMP_ADD(
                d.sim_day0,
                INTERVAL CAST(SPLIT(pr.schedule_interval, ' ')[OFFSET(1)] AS INT64) HOUR
              ),
              INTERVAL CAST(SPLIT(pr.schedule_interval, ' ')[OFFSET(0)] AS INT64) MINUTE
            ),
            d.sim_day7,
            INTERVAL 1 DAY
          )
        ELSE ARRAY<TIMESTAMP>[]
      END
    )
  ) AS run_ts
  WHERE run_ts < d.sim_day7
),

-- Apply offsets
timed AS (
  SELECT
    cluster_name,
    dag_id,
    test_id,
    dag_run_start AS dag_sim_start,
    TIMESTAMP_ADD(
      dag_run_start,
      INTERVAL CAST(ROUND(dag_duration_seconds, 0) AS INT64) SECOND
    ) AS dag_sim_end,
    TIMESTAMP_ADD(
      dag_run_start,
      INTERVAL CAST(ROUND(start_offset_seconds, 0) AS INT64) SECOND
    ) AS test_sim_start_time,
    TIMESTAMP_ADD(
      TIMESTAMP_ADD(
        dag_run_start,
        INTERVAL CAST(ROUND(start_offset_seconds, 0) AS INT64) SECOND
      ),
      INTERVAL CAST(ROUND(test_duration_seconds, 0) AS INT64) SECOND
    ) AS test_sim_end_time
  FROM expanded
)

-- Final output
SELECT
  t.cluster_name,
  t.dag_id,
  d.schedule_interval,
  d.formatted_schedule,  
  t.test_id,
  t.dag_sim_start,
  t.dag_sim_end,
  t.test_sim_start_time,
  t.test_sim_end_time,
  (
    SELECT COUNT(1)
    FROM timed s
    WHERE s.cluster_name = t.cluster_name
      AND s.test_sim_start_time < t.test_sim_end_time
      AND s.test_sim_end_time   > t.test_sim_start_time
  ) AS concurrent_tests
FROM timed t
JOIN dag_sch d
  ON t.dag_id = d.dag_id

