CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_2.dag_run_status` AS
WITH
  dag_with_tag AS (
    SELECT
      dt.dag_id,
      ARRAY_AGG(name) AS tags
    FROM
      `amy_xlml_poc_2.dag_tag` AS dt
    GROUP BY
      dag_id
  ),
  dag_runs AS (
    SELECT
      dr.dag_id,
      dr.run_id,
      dr.start_date
    FROM
      `amy_xlml_poc_2.dag_run` AS dr
      JOIN `amy_xlml_poc_2.dag` AS dag
        ON dag.dag_id = dr.dag_id
    WHERE
      dr.dag_id NOT IN ('airflow_monitoring', 'clean_up', 'airflow_to_bq_export','on_failure_actions_trigger')
  ),
  last_task_status AS (
    SELECT
      ti.dag_id,
      ti.run_id,
      ti.task_id,
      ti.state,
      ROW_NUMBER() OVER (PARTITION BY ti.dag_id, ti.run_id, ti.task_id ORDER BY ti.try_number DESC) AS rn
    FROM
      `amy_xlml_poc_2.task_instance` AS ti
      JOIN dag_runs AS dr
        ON ti.dag_id = dr.dag_id
        AND ti.run_id = dr.run_id
  ),
  task_status AS (
    SELECT
      dag_id,
      run_id,
      COUNT(DISTINCT task_id) AS total_tasks,
      SUM(CASE WHEN state = 'success' THEN 1 ELSE 0 END) AS successful_tasks
    FROM last_task_status
    WHERE rn = 1
    GROUP BY
      dag_id,
      run_id
  ),
  dag_run_results AS (
    SELECT
      dag_id,
      run_id,
      total_tasks,
      successful_tasks,
      CASE
        WHEN total_tasks = successful_tasks THEN 1
        ELSE 0
      END AS is_passed
    FROM
      task_status
  ),
  top_level_tests AS (
    SELECT
      dag_id,
      COUNT(DISTINCT SPLIT(task_id, '.')[
        OFFSET(0)
      ]) AS num_tests
    FROM
      `amy_xlml_poc_2.task_instance`
    GROUP BY
      dag_id
  ),
  dag_cleaned_owners AS (
    SELECT
      dag_id,
      STRING_AGG(DISTINCT TRIM(part)) AS cleaned_owners
    FROM (
      SELECT
        dag_id,
        part
      FROM
        `amy_xlml_poc_2.dag`,
        UNNEST(SPLIT(owners, ',')) AS part
      WHERE
        LOWER(TRIM(part)) != 'airflow'
    )
    GROUP BY
      dag_id
  ),
  full_dag_runs AS (
    SELECT
      drr.dag_id,
      drr.run_id,
      drr.is_passed,
      DATE(dr.start_date) AS run_date,
      dc.cleaned_owners AS dag_owner,
      dwt.tags
    FROM
      dag_run_results AS drr
      LEFT JOIN dag_runs AS dr
        ON drr.dag_id = dr.dag_id
        AND drr.run_id = dr.run_id
      LEFT JOIN dag_cleaned_owners AS dc
        ON drr.dag_id = dc.dag_id
      LEFT JOIN dag_with_tag AS dwt
        ON drr.dag_id = dwt.dag_id
  ),
  daily_dag_status AS (
    SELECT
      fdr.dag_id,
      fdr.dag_owner,
      fdr.tags,
      fdr.run_date AS date,
      CASE
        WHEN COUNT(fdr.run_id) = 0 THEN 'no run'
        WHEN COUNTIF(fdr.is_passed = 0) > 0 THEN 'failed'
        ELSE 'success'
      END AS status
    FROM
      full_dag_runs AS fdr
    GROUP BY
      fdr.dag_id,
      fdr.dag_owner,
      fdr.tags,
      fdr.run_date
  ),
  distinct_dag_details AS (
    SELECT DISTINCT
      dag_id,
      dag_owner,
      tags
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
  ds.date,
  COALESCE(dds.status, 'no run') AS status
FROM
  distinct_dag_details AS ddd
CROSS JOIN
  date_series AS ds
LEFT JOIN
  daily_dag_status AS dds
ON
  ddd.dag_id = dds.dag_id
  AND ds.date = dds.date

