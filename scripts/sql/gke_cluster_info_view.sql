CREATE OR REPLACE VIEW `cienet-cmcs.amy_xlml_poc_2.gke_cluster_info_view` AS
WITH TestAggregated AS (
    SELECT
        t2.project_name,
        t2.cluster_name,
        ARRAY_AGG(
            STRUCT(
                t2.dag_id,
                t2.run_id,
                t2.test_id
            )
        ) AS tests_in_use
    FROM
        `cienet-cmcs.amy_xlml_poc_2.cluster_info_view_latest` AS t2
    GROUP BY
        t2.project_name,
        t2.cluster_name
)
SELECT
    t1.*,
    t3.tests_in_use
FROM
    `cienet-cmcs.amy_xlml_poc_2.gke_cluster_info` AS t1
LEFT JOIN
    TestAggregated AS t3
ON
    t1.project_id = t3.project_name
    AND t1.cluster_name = t3.cluster_name;

