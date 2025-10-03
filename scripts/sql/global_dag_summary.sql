CREATE OR REPLACE VIEW `amy_xlml_poc_prod.global_dag_summary` AS

SELECT
  t1.data_start,
  t1.data_end,
  t1.total_dags,
  t1.total_dags_q,
  t1.category_counts,
  t1.accelerator_counts,
  t1.total_cluster_dag,
  t1.total_runs, 
  t1.passed_runs,
  t1.partial_passed_runs,
  t1.total_tests,
  t1.total_tests_qe,
  t1.total_tests_q,
  t1.total_cluster_test,
  t1.total_clusters,
  t1.total_projects,
  t2.abnormal_cluster,
  t2.abnormal_nodepool,
  t2.cluster_check_time,
  t2.abnormal_cluster_counts,
  t1.project_cluster_counts,
  t1.cluster_counts
FROM 
  `amy_xlml_poc_prod.global_dag_summary_view` t1,
  `amy_xlml_poc_prod.global_cluster_view` t2



