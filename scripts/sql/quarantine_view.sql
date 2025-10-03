CREATE OR REPLACE VIEW `amy_xlml_poc_prod.quarantine_view` AS
WITH tasks AS (
    -- 1. Unnest the task_order array to flatten the structure
    SELECT
        t1.dag_id,
        t2.task_id,
        t2.parents
    FROM
        `amy_xlml_poc_prod.dag_task_order` AS t1,
        UNNEST(t1.task_order) AS t2
),
test_quarantine AS (
    -- 2. Extract the test_id and determine the Quarantine status
    SELECT
        dag_id,
        -- Extract the first part of the task_id as the test_id
        SPLIT(task_id, '.')[OFFSET(0)] AS test_id,
        
        -- Check if any parent task group contains the string "Quarantine"
        (
            SELECT 
                COUNTIF(parent_task_group LIKE 'Quarantine') > 0
            FROM 
                UNNEST(parents) AS parent_task_group
        ) AS is_quarantine_per_task
    FROM
        tasks
),
res AS (
-- 3. Aggregate to the (dag_id, test_id) level
--    and resolve the flag: TRUE if *any* task is marked as quarantined.
SELECT
    dag_id,
    test_id,
    -- MAX(is_quarantine_per_task) will be TRUE if at least one record is TRUE
    -- (since TRUE is treated as 1 and FALSE as 0 in BigQuery aggregation).
    MAX(is_quarantine_per_task) AS is_quarantine
FROM
    test_quarantine
GROUP BY
    1, 2 -- Group by dag_id, test_id
)

SELECT * from res
--SELECT
--    dag_id,
--    COUNT(DISTINCT test_id) AS total_tests,
--    COUNTIF(is_quarantine) AS quarantine_tests,
--    ARRAY_AGG(
--        CASE 
--            WHEN is_quarantine THEN test_id 
--            ELSE NULL 
--        END 
--        IGNORE NULLS 
--        ORDER BY test_id
--    ) AS quarantines
--FROM res
--GROUP BY dag_id

