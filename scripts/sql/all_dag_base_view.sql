
CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_prod.all_dag_base_view` AS
WITH 

all_dags AS (
  SELECT d.*
  FROM `cienet-cmcs.amy_xlml_poc_prod.dag` d
  WHERE last_parsed_time >= TIMESTAMP(DATE_SUB(CURRENT_DATE('UTC'), INTERVAL 1 DAY)) AND is_active=TRUE AND
    dag_id NOT IN (SELECT dag_id from `cienet-cmcs.amy_xlml_poc_prod.config_ignore_dags`)
--  WHERE dag_id IN
--  (SELECT dag_id FROM `cienet-cmcs.cienet-cmcs.amy_xlml_poc_prod.serialized_dag`)
--  AND dag_id NOT IN (SELECT dag_id from `cienet-cmcs.amy_xlml_poc_prod.config_ignore_dags`)
),

dag_sch AS (
  SELECT
    dag_id,
    CASE
      WHEN schedule_interval IS NULL OR schedule_interval = '' THEN schedule_interval
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
    `cienet-cmcs.amy_xlml_poc_prod.dag` 
  WHERE dag_id in (SELECT DISTINCT dag_id FROM all_dags)  
),

-- DAG owners cleaned (removes "airflow")
dag_cleaned_owners AS (
  SELECT
    dag_id,
    STRING_AGG(DISTINCT TRIM(part)) AS cleaned_owners
  FROM (
    SELECT
      dag_id,
      part
    FROM `cienet-cmcs.amy_xlml_poc_prod.dag`,
    UNNEST(SPLIT(owners, ',')) AS part
    WHERE LOWER(TRIM(part)) != 'airflow'
  )
  GROUP BY dag_id
),

-- DAGs with the specified tag
dag_with_tag AS (
  SELECT dt.dag_id,ARRAY_AGG(name) as tags
  FROM `cienet-cmcs.amy_xlml_poc_prod.dag_tag` dt
  GROUP BY dag_id  
),

-- Create a list of all dag_id and category matches
category_matches AS (
  SELECT
    d.dag_id,
    c.name AS category,
    c.pri_order,
    ROW_NUMBER() OVER (PARTITION BY d.dag_id ORDER BY c.pri_order) AS rn
  FROM
    dag_with_tag d
  CROSS JOIN
    `cienet-cmcs.amy_xlml_poc_prod.config_category` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    lower(tag1) = lower(tag2)
),

-- Create a list of all dag_id and accelerator matches
accel_matches AS (
  SELECT
    d.dag_id,
    c.name AS accelerator,
    c.pri_order,
    ROW_NUMBER() OVER (PARTITION BY d.dag_id ORDER BY c.pri_order) AS rn
  FROM
    dag_with_tag d
  CROSS JOIN
    `cienet-cmcs.amy_xlml_poc_prod.config_accelerator` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    lower(tag1) = lower(tag2)
),
accelerator_categorization AS (
  WITH all_accelerators AS (
     SELECT
      ta.dag_id,
      accelerator_family AS accelerator
    FROM
      `cienet-cmcs.amy_xlml_poc_prod.tests_accelerator_view` ta
    LEFT JOIN `cienet-cmcs.amy_xlml_poc_prod.quarantine_view` q ON ta.dag_id=q.dag_id and ta.test_id=q.test_id
    WHERE
      accelerator_family IS NOT NULL AND (q.is_quarantine IS NULL OR q.is_quarantine = FALSE)
    UNION DISTINCT
    SELECT
      dag_id,
      accelerator
    FROM
      accel_matches
    WHERE
      accelerator IS NOT NULL AND rn = 1
  )
  
  SELECT
    dag_id,
    --ARRAY_AGG(DISTINCT accelerator) AS unique_accelerators,
    STRING_AGG(
      DISTINCT accelerator, 
      '+' 
      ORDER BY accelerator
    ) AS accelerator
  FROM
    all_accelerators
  GROUP BY
    dag_id
)

SELECT
    d.dag_id,
    d.is_paused,
    d.schedule_interval,
    REGEXP_REPLACE(ds.formatted_schedule, r'^"|"$', '') AS formatted_schedule,
    b.tags,
    dco.cleaned_owners AS dag_owners, 
    p.category,
    a.accelerator AS aggr_accelerator,
    m.accelerator AS tag_accelerator,
    m.accelerator AS accelerator,
    --a.accelerator AS tag_accelerator,
    --m.accelerator,
    d.description
  FROM all_dags d
  LEFT JOIN category_matches p ON d.dag_id = p.dag_id AND p.rn = 1
  LEFT JOIN accelerator_categorization a ON d.dag_id = a.dag_id 
  LEFT JOIN accel_matches m ON d.dag_id = m.dag_id AND m.rn = 1
  LEFT JOIN dag_with_tag b ON d.dag_id=b.dag_id
  LEFT JOIN dag_cleaned_owners dco ON d.dag_id = dco.dag_id
  LEFT JOIN dag_sch ds ON d.dag_id = ds.dag_id
 
