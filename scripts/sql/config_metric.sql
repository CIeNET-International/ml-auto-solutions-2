
CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_metric_template` (
    metric_name STRING,
    metric_disp_name STRING,
    api_metric_type STRING,
    page_state_template STRING
);


INSERT INTO `cienet-cmcs.amy_xlml_poc_prod.config_metric_template` 
(metric_name, metric_disp_name, api_metric_type, page_state_template)
VALUES
(
    'node_pool_status',
    'Node-pool Status',
    'kubernetes.io/node_pool/status',
    '{"xyChart":{"constantLines":[],"dataSets":[{"plotType":"STACKED_AREA","pointConnectionMethod":"GAP_DETECTION","targetAxis":"Y1","timeSeriesFilter":{"aggregations":[{"alignmentPeriod":"60s","crossSeriesReducer":"REDUCE_NONE","groupByFields":[],"perSeriesAligner":"ALIGN_FRACTION_TRUE"}],"apiSource":"DEFAULT_CLOUD","crossSeriesReducer":"REDUCE_NONE","filter":"metric.type=\\\"kubernetes.io/node_pool/status\\\" resource.type=\\\"k8s_node_pool\\\" resource.label.\\\"project_id\\\"=\\\"{{PROJECT}}\\\" resource.label.\\\"node_pool_name\\\"=\\\"{{POOL}}\\\" resource.label.\\\"cluster_name\\\"=\\\"{{CLUSTER}}\\\"","groupByFields":[],"minAlignmentPeriod":"60s","perSeriesAligner":"ALIGN_FRACTION_TRUE","pickTimeSeriesFilter":{"direction":"TOP","numTimeSeries":30,"rankingMethod":"METHOD_MEAN"}}}],"options":{"mode":"COLOR"},"y1Axis":{"label":"","scale":"LINEAR"}}}'
),
(
    'node_pool_accelerator_ttr',
    'GKE NodePool Time-To_Recover TTR',
    'kubernetes.io/node_pool/accelerator/times_to_recover',
    '{"xyChart":{"constantLines":[],"dataSets":[{"plotType":"STACKED_BAR","pointConnectionMethod":"GAP_DETECTION","targetAxis":"Y1","timeSeriesFilter":{"aggregations":[{"alignmentPeriod":"60s","crossSeriesReducer":"REDUCE_MEAN","groupByFields":[],"perSeriesAligner":"ALIGN_SUM"}],"apiSource":"DEFAULT_CLOUD","crossSeriesReducer":"REDUCE_MEAN","filter":"metric.type=\\\"kubernetes.io/node_pool/accelerator/times_to_recover\\\" resource.type=\\\"k8s_node_pool\\\" resource.label.\\\"project_id\\\"=\\\"{{PROJECT}}\\\" resource.label.\\\"cluster_name\\\"=\\\"{{CLUSTER}}\\\" resource.label.\\\"node_pool_name\\\"=\\\"{{POOL}}\\\"","groupByFields":[],"minAlignmentPeriod":"60s","perSeriesAligner":"ALIGN_SUM","pickTimeSeriesFilter":{"direction":"TOP","numTimeSeries":30,"rankingMethod":"METHOD_MEAN"}}}],"options":{"mode":"COLOR"},"y1Axis":{"label":"","scale":"LINEAR"}}}'
),
 (
    'multi_host_available',
    'Node-pool Availability',
    'kubernetes.io/node_pool/multi_host/available',
    '{"xyChart":{"constantLines":[],"dataSets":[{"plotType":"LINE","pointConnectionMethod":"GAP_DETECTION","targetAxis":"Y1","timeSeriesFilter":{"aggregations":[{"alignmentPeriod":"60s","crossSeriesReducer":"REDUCE_MEAN","groupByFields":[],"perSeriesAligner":"ALIGN_FRACTION_TRUE"}],"apiSource":"DEFAULT_CLOUD","crossSeriesReducer":"REDUCE_MEAN","filter":"metric.type=\\\"kubernetes.io/node_pool/multi_host/available\\\" resource.type=\\\"k8s_node_pool\\\" resource.label.\\\"project_id\\\"=\\\"{{PROJECT}}\\\" resource.label.\\\"cluster_name\\\"=\\\"{{CLUSTER}}\\\" resource.label.\\\"node_pool_name\\\"=\\\"{{POOL}}\\\"","groupByFields":[],"minAlignmentPeriod":"60s","perSeriesAligner":"ALIGN_FRACTION_TRUE","pickTimeSeriesFilter":{"direction":"TOP","numTimeSeries":30,"rankingMethod":"METHOD_MEAN"}}}],"options":{"mode":"COLOR"},"y1Axis":{"label":"","scale":"LINEAR"}}}'
);


metric required parameter issues:
1. JobSet related: need entity_name    
    JobSet Time To Recover (metric.type="kubernetes.io/jobset/times_to_recover", it's workload level metric): require entity_name
    JobSet Time Between Interruptions
    JobSet Healthiness
    JobSet Uptime
2. Memory Utilization / Duty Cycle/ Tensor Core Utilization / DCN transfer Lanencies related:need pod_name, (namespace_name?), (container_name?)






CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_test_metric` (
    metric_name STRING,
    dag_id STRING,
    test_id STRING,
    is_active BOOLEAN
);

INSERT INTO `cienet-cmcs.amy_xlml_poc_prod.config_test_metric` 
(metric_name, dag_id, test_id, is_active)
VALUES
(
    'node_pool_status',
    'gke_node_pool_status',
    'v6e',
    TRUE
),
(
    'node_pool_accelerator_ttr',
    'node_pool_ttr_update_label',
    'v6e',
    TRUE
),
(
    'multi_host_available',
    'multi-host-availability-rollback',
    'v6e',
    TRUE
),
(
    'multi_host_available',
    'gke_node_pool_label_update',
    'v6e',
    TRUE
);



