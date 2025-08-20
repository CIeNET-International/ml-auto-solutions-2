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

-- 0) anchor: most-recent logical run per DAG
last_run AS (
  SELECT
    dr.dag_id,
    MAX(dr.execution_date) AS last_exec
  FROM `amy_xlml_poc_2.dag_run` dr
  GROUP BY dr.dag_id
),

-- 1) Normalize schedule + build a profile STRUCT (so unions can be consistent)
norm AS (
  SELECT
    pr.*,                            -- keep original profile columns in the row
    (SELECT AS STRUCT pr.*) AS profile,  -- <--- fixed here
    d.sim_day0,
    d.sim_day7,
    TRIM(pr.schedule_interval, '"') AS sched,

    -- cron detection and fields
    REGEXP_CONTAINS(TRIM(pr.schedule_interval, '"'),
      r'^\s*\d+\s+\d+\s+\*\s+\*\s+(?:[\d,]+|\*)\s*$') AS is_cron,
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r'^\s*(\d+)') AS INT64) AS cron_minute,
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r'^\s*\d+\s+(\d+)') AS INT64) AS cron_hour,
    REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r'\s+\*\s+\*\s+([\d,]*|\*)\s*$') AS cron_dow_raw,

    -- timedelta from JSON (preferred)
    SAFE_CAST(JSON_EXTRACT_SCALAR(TRIM(pr.schedule_interval, '"'), '$.days') AS INT64) AS td_days_json,
    SAFE_CAST(JSON_EXTRACT_SCALAR(TRIM(pr.schedule_interval, '"'), '$.seconds') AS INT64) AS td_seconds_json,
    SAFE_CAST(JSON_EXTRACT_SCALAR(TRIM(pr.schedule_interval, '"'), '$.microseconds') AS INT64) AS td_usecs_json,

    -- fallback: parse formatted string like "1 day, 00:00:00"
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r'^\s*(\d+)\s+day') AS INT64) AS td_days_str,
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r',\s*(\d{1,2}):\d{2}:\d{2}') AS INT64) AS td_h_str,
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r',\s*\d{1,2}:(\d{2}):\d{2}') AS INT64) AS td_m_str,
    SAFE_CAST(REGEXP_EXTRACT(TRIM(pr.schedule_interval, '"'), r',\s*\d{1,2}:\d{2}:(\d{2})') AS INT64) AS td_s_str

  FROM profiles pr
  CROSS JOIN day0 d
),

-- 2) produce safe dow_allowed array (never attempts to cast '*' to INT64)
dow_norm AS (
  SELECT
    n.*,
    CASE
      WHEN n.cron_dow_raw = '*' THEN [0,1,2,3,4,5,6]
      WHEN n.cron_dow_raw IS NULL THEN []
      ELSE ARRAY(
        SELECT CAST(tok AS INT64)
        FROM UNNEST(SPLIT(REGEXP_REPLACE(n.cron_dow_raw, r'\s+', ''), ',')) AS tok
        WHERE SAFE_CAST(tok AS INT64) IS NOT NULL
      )
    END AS cron_dow_allowed
  FROM norm n
),


-- 3) compute td interval seconds and join last_run (keeps profile struct)
td_norm AS (
  SELECT
    dn.*,
    COALESCE(
      -- JSON timedelta
      CASE WHEN dn.td_days_json IS NOT NULL OR dn.td_seconds_json IS NOT NULL OR dn.td_usecs_json IS NOT NULL THEN
        COALESCE(dn.td_days_json,0)*86400
        + COALESCE(dn.td_seconds_json,0)
        + CAST(COALESCE(dn.td_usecs_json,0) / 1000000 AS INT64)
      END,
      -- formatted-string fallback
      CASE WHEN dn.td_days_str IS NOT NULL THEN
        COALESCE(dn.td_days_str,0)*86400
        + COALESCE(dn.td_h_str,0)*3600
        + COALESCE(dn.td_m_str,0)*60
        + COALESCE(dn.td_s_str,0)
      END
    ) AS td_interval_secs,
    lr.last_exec
  FROM dow_norm dn
  LEFT JOIN last_run lr USING (dag_id)
),

-- 4) @daily branch  -> each row yields (profile, dag_run_start)
daily_runs AS (
  SELECT profile, run_ts AS dag_run_start
  FROM td_norm
  CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(sim_day0, sim_day7, INTERVAL 1 DAY)) AS run_ts
  WHERE sched = '@daily' AND run_ts < sim_day7
),

-- 5) @hourly branch
hourly_runs AS (
  SELECT profile, run_ts AS dag_run_start
  FROM td_norm
  CROSS JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(sim_day0, sim_day7, INTERVAL 1 HOUR)) AS run_ts
  WHERE sched = '@hourly' AND run_ts < sim_day7
),

-- 6) cron branch: m h * * *  OR  m h * * d,d,d
cron_runs AS (
  SELECT
    profile,
    TIMESTAMP_ADD(TIMESTAMP_ADD(day_ts, INTERVAL cron_hour HOUR), INTERVAL cron_minute MINUTE) AS dag_run_start
  FROM td_norm,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(sim_day0, sim_day7, INTERVAL 1 DAY)) AS day_ts
  WHERE is_cron
    AND cron_minute IS NOT NULL AND cron_hour IS NOT NULL
    AND (EXTRACT(DAYOFWEEK FROM TIMESTAMP_ADD(TIMESTAMP_ADD(day_ts, INTERVAL cron_hour HOUR), INTERVAL cron_minute MINUTE)) - 1)
        IN UNNEST(cron_dow_allowed)
    AND TIMESTAMP_ADD(TIMESTAMP_ADD(day_ts, INTERVAL cron_hour HOUR), INTERVAL cron_minute MINUTE) < sim_day7
),

-- 7) timedelta branch: seed from last_exec (logical) or sim_day0, then seed + n*step
timedelta_runs AS (
  SELECT profile, ts AS dag_run_start
  FROM (
    SELECT
      t.*,
      COALESCE(t.last_exec, t.sim_day0) AS seed_ts,
      t.td_interval_secs AS step_secs
    FROM td_norm t
    WHERE t.td_interval_secs IS NOT NULL AND t.td_interval_secs > 0
  ) p
  -- compute first multiplier n0 such that seed + n0*step >= sim_day0 (n0 >= 1)
  CROSS JOIN UNNEST(ARRAY<INT64>[
    GREATEST(1, CAST(CEIL(SAFE_DIVIDE(TIMESTAMP_DIFF(p.sim_day0, p.seed_ts, SECOND), p.step_secs)) AS INT64))
  ]) AS n0
  CROSS JOIN UNNEST(
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_ADD(p.seed_ts, INTERVAL n0 * p.step_secs SECOND),
      p.sim_day7,
      INTERVAL p.step_secs SECOND
    )
  ) AS ts
  WHERE ts < p.sim_day7
),

-- 8) union everything into a consistent two-column intermediate, then unpack profile
union_intermediate AS (
  SELECT profile, dag_run_start FROM daily_runs
  UNION ALL
  SELECT profile, dag_run_start FROM hourly_runs
  UNION ALL
  SELECT profile, dag_run_start FROM cron_runs
  UNION ALL
  SELECT profile, dag_run_start FROM timedelta_runs
),

-- 9) final expanded: expand profile struct back to original profile columns + dag_run_start
expanded AS (
  SELECT (profile).* , dag_run_start
  FROM union_intermediate
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


