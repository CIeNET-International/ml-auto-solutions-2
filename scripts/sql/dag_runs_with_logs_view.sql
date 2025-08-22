CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_2.dag_runs_with_logs_view` AS
SELECT
    dr.dag_id,
    dr.run_id,
    dr.execution_date,
    dr.run_start_date,
    dr.run_end_date,
    dr.run_status,
    ARRAY(
        SELECT AS STRUCT
            t.test_id,
            t.test_start_date,
            t.test_end_date,
            t.test_status,
            TO_JSON_STRING(t.logs) AS logs_string, -- Converts the entire logs array to a JSON string
            t.log_url_error,
            t.log_url_all
        FROM
            UNNEST(dr.tests) AS t
    ) AS tests
FROM
    `cienet-cmcs.amy_xlml_poc_2.dag_runs_with_logs` AS dr;
