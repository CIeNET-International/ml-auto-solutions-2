
CREATE OR REPLACE VIEW `amy_xlml_poc_prod.all_dag_base_view` AS
WITH 

all_dags AS (
  SELECT d.*
  FROM `amy_xlml_poc_prod.dag` d
  WHERE dag_id IN
  (SELECT dag_id FROM `cienet-cmcs.amy_xlml_poc_prod.serialized_dag`)
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
    `amy_xlml_poc_prod.dag` 
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
    FROM `amy_xlml_poc_prod.dag`,
    UNNEST(SPLIT(owners, ',')) AS part
    WHERE LOWER(TRIM(part)) != 'airflow'
  )
  GROUP BY dag_id
),

-- DAGs with the specified tag
dag_with_tag AS (
  SELECT dt.dag_id,ARRAY_AGG(name) as tags
  FROM `amy_xlml_poc_prod.dag_tag` dt
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
    `amy_xlml_poc_prod.config_category` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    tag1 = tag2
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
    `amy_xlml_poc_prod.config_accelerator` c,
    UNNEST(d.tags) AS tag1,
    UNNEST(c.tag_names) AS tag2
  WHERE
    tag1 = tag2
)

SELECT
    d.dag_id,
    b.is_paused,
    b.schedule_interval,
    ds.formatted_schedule,
    d.tags,
    dco.cleaned_owners AS dag_owners, 
    p.category,
    a.accelerator,
    b.description
  FROM dag_with_tag d
  LEFT JOIN category_matches p ON d.dag_id = p.dag_id AND p.rn = 1
  LEFT JOIN accel_matches a ON d.dag_id = a.dag_id AND a.rn = 1
  JOIN all_dags b ON d.dag_id=b.dag_id
  LEFT JOIN dag_cleaned_owners dco ON d.dag_id = dco.dag_id
  LEFT JOIN dag_sch ds ON d.dag_id = ds.dag_id
 



