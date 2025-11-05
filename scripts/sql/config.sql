
CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_ignore_dags` (
    dag_id STRING
);

insert into `cienet-cmcs.amy_xlml_poc_prod.config_ignore_dags` (dag_id) values
    ('airflow_to_bq_export'),
    ('airflow_monitoring'),
    ('on_failure_actions_trigger'),
    ('clean_up'),
    ('xlml_to_buganizer'),
    ('gke_example_dag'),
    ('maxtext_sweep_gke_example_dag'),
    ('maxtext_profile_sweep_example_dag'),
    ('maxtext_profile_namegen_example_dag'),
    ('maxtext_benchmark_once'),
    ('tf_dlrm_2_18'),
    ('tf_2_18_supported'),
    ('tf_2_18_se_supported'),
    ('tf_nightly_supported');



CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_window` (
    type STRING,
    value INTEGER
);

insert into `cienet-cmcs.amy_xlml_poc_prod.config_window` (type, value) values 
    ('d', 1),('d', 3),('d', 7),('d',30),('d',60),
    ('r', 1),('r', 2),('r', 3),('r', 4),('r', 5),('r', 6),('r', 7),
    ('the_r', 1),('the_r', 2),('the_r', 3),('the_r', 4),('the_r', 5),('the_r', 6),('the_r', 7),('the_r', 8),('the_r', 9),('the_r', 10),
    ('the_r', 11),('the_r', 12),('the_r', 13),('the_r', 14),('the_r', 15),('the_r', 16),('the_r', 17),('the_r', 18),('the_r', 19),('the_r', 20),
    ('the_r', 21),('the_r', 22),('the_r', 23),('the_r', 24),('the_r', 25),('the_r', 26),('the_r', 27),('the_r', 28),('the_r', 29),('the_r', 30),
    ('the_r', 31),('the_r', 32),('the_r', 33),('the_r', 34),('the_r', 35),('the_r', 36),('the_r', 37),('the_r', 38),('the_r', 39),('the_r', 40),
    ('the_r', 41),('the_r', 42),('the_r', 43),('the_r', 44),('the_r', 45),('the_r', 46),('the_r', 47),('the_r', 48),('the_r', 49),('the_r', 50),    
    ('the_r', 51),('the_r', 52),('the_r', 53),('the_r', 54),('the_r', 55),('the_r', 56),('the_r', 57),('the_r', 58),('the_r', 59),('the_r', 60),    
    ('the_d', 1),('the_d', 2),('the_d', 3),('the_d', 4),('the_d', 5),('the_d', 6),('the_d', 7),('the_d', 8),('the_d', 9),('the_d', 10),
    ('the_d', 11),('the_d', 12),('the_d', 13),('the_d', 14),('the_d', 15),('the_d', 16),('the_d', 17),('the_d', 18),('the_d', 19),('the_d', 20),
    ('the_d', 21),('the_d', 22),('the_d', 23),('the_d', 24),('the_d', 25),('the_d', 26),('the_d', 27),('the_d', 28),('the_d', 29),('the_d', 30), 
    ('the_d', 31),('the_d', 32),('the_d', 33),('the_d', 34),('the_d', 35),('the_d', 36),('the_d', 37),('the_d', 38),('the_d', 39),('the_d', 40),
    ('the_d', 41),('the_d', 42),('the_d', 43),('the_d', 44),('the_d', 45),('the_d', 46),('the_d', 47),('the_d', 48),('the_d', 49),('the_d', 50),    
    ('the_d', 51),('the_d', 52),('the_d', 53),('the_d', 54),('the_d', 55),('the_d', 56),('the_d', 57),('the_d', 58),('the_d', 59),('the_d', 60), 
    ('the_w', 1),('the_w', 2),('the_w', 3),('the_w', 4),('the_w', 5),('the_w', 6),('the_w', 7),('the_w', 8),
    ('the_wu', 1),('the_wu', 2),('the_wu', 3),('the_wu', 4),('the_wu', 5),('the_wu', 6),('the_wu', 7),('the_wu', 8);



CREATE OR REPLACE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_category` (
    name STRING,
    tag_names ARRAY<STRING>,
    pri_order INTEGER
);


INSERT INTO `cienet-cmcs.amy_xlml_poc_prod.config_category` (name, tag_names, pri_order) values 
('MaxText',['maxtext'],10),
('TPU-Obs',['tpu-observability'],1),
('Orbax',['orbax'],1),
('Pathways',['pathways'],1);



CREATE OR REPLACE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_accelerator` (
    name STRING,
    tag_names ARRAY<STRING>,
    pri_order INTEGER
);

INSERT INTO `cienet-cmcs.amy_xlml_poc_prod.config_accelerator`(name, tag_names, pri_order) values 
('GPU',['a3mega','a3ultra', 'a4', 'gpu'],1),
('TPU', ['tpu'], 1);



CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_ignore_skipped_dags` (
    dag_id STRING
);

--insert into `cienet-cmcs.amy_xlml_poc_prod.config_ignore_skipped_dags` (dag_id)  from (
--  SELECT dag_id FROM `cienet-cmcs.amy_xlml_poc_prod.all_dag` where dag_id like '%interruption%'
--)

insert into `cienet-cmcs.amy_xlml_poc_prod.config_ignore_skipped_dags` (dag_id) values
('validate_interruption_count_gce_migrate_on_hwsw_maintenance'),
('validate_interruption_count_gke_migrate_on_hwsw_maintenance'),
('validate_interruption_count_gce_defragmentation'),
('validate_interruption_count_gce_hwsw_maintenance'),
('validate_interruption_count_gke_host_error'),
('validate_interruption_count_gce_bare_metal_preemption'),
('validate_interruption_count_gce_host_error'),
('validate_interruption_count_gke_other'),
('validate_interruption_count_gke_defragmentation'),
('validate_interruption_count_gke_bare_metal_preemption'),
('validate_interruption_count_gce_eviction'),
('validate_interruption_count_gke_hwsw_maintenance'),
('validate_interruption_count_gke_eviction'),
('validate_interruption_count_gce_other');


---------------------------------------
CREATE TABLE `cienet-cmcs.amy_xlml_poc_prod.config_error` (
    err_code STRING,
    err_regx STRING,
    err_category STRING,
    err_type STRING,
    err_short_desc STRING,
    remark STRING
    
);

insert into `cienet-cmcs.amy_xlml_poc_prod.config_error` (err_regx, err_code, err_category, err_type) values 
("airflow.exceptions.AirflowFailException: Bad pod phase: Failed",1010,"Airflow","airflow.exception"),
("airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of .* seconds exceeds the specified timeout of .*",1020,"Airflow","airflow.exception"),
("AssertionError: Mantaray command failed with code 1",2110,"DAG","AssertionError"),
,,2111,,AssertionError (1),ModuleNotFoundError: No module named 'mantaray'
("AssertionError: XPK clean-up failed with code 1,,2120,DAG (2),AssertionError (1),
("AssertionError: XPK command failed with code 1,,2130,DAG (2),AssertionError (1),\[XPK\] b'ERROR: \(gcloud\.container\.images\.describe\) \[.*?\] is not found or is not a valid name\.
,,2131,,AssertionError (1),"\[XPK\] b'error: the server doesn't have a resource type ""workloads"""
,,2132,,AssertionError (1),"\[XPK\] b'Error: unknown command "".*?"" for ""kubectl"""
("AssertionError: XPK command failed with code 2,,2140,DAG (2),,
("AttributeError: Unknown field for Operation: exception,,2210,DAG (2),AttributeError (2),
f("abric.exceptions.GroupException: {<Connection host=[\d\.]+>: <UnexpectedExit: cmd='.*?' exited=\d+>},,2310,DAG (2),fabric.exception (3),
("fabric.exceptions.GroupException: {<Connection host=[\d\.]+ user=cloud-ml-auto-solutions>: TimeoutError\(110, 'Connection timed out'\)}",,2320,DAG (2),fabric.exception (3),
("fabric\.exceptions\.GroupException: {<Connection host=[\d\.]+ user=cloud-ml-auto-solutions>: AuthenticationException\('Authentication failed\.'\)},,2330,DAG (2),fabric.exception (3),
("IndexError: list index out of range,,2410,DAG (2),Index Error (4),"Error: INSTALLATION FAILED: path "".*?"" not found"
,,2411,,Index Error (4),ImportError: .*?\.so: undefined symbol: \w+
("RuntimeError: Bad queued resource state FAILED,,2510,DAG (2),Runtime Error (5),
("RuntimeError: Event count mismatch.,,2520,DAG (2),Runtime Error (5),
("RuntimeError: Non-zero exit code,,2530,,Runtime Error (5),fatal: destination path '.*?' already exists and is not an empty directory\.
,,2531,,Runtime Error (5),ImportError: .*?\.so: undefined symbol: \w+
("subprocess.CalledProcessError: Command 'gcloud container node-pools (create|delete) .*' returned non-zero exit status 1.,,2610,DAG (2),CallProcessError (6),
("tensorflow.python.framework.errors_impl.NotFoundError: \{\{.*?\}\} /tmp/tmp\w+/tflog/metrics; No such file or directory [Op:IteratorGetNext] name:,,2710,DAG (2),tensorflow (7),
("TypeError: a bytes-like object is required, not 'str'",,2810,DAG (2),TypeError (8),
("TypeError: Object of type \w+ is not JSON serializable,,2820,DAG (2),TypeError (8),
("google.api_core.exceptions.NotFound: 404 GET https:\/\/storage\.googleapis\.com\/download\/storage\/v1\/b\/ml-auto-solutions\/o\/.*?: No such object: .*?,,9010,ENV (9),google.api_core.exception (0),
("google.api_core.exceptions.PermissionDenied: 403.*,,9020,ENV (9),google.api_core.exception (0),
("google.api_core.exceptions.ResourceExhausted: 429 Quota limit 'QueuedResourcePerProjectPerZone' has been exceeded.*,,9030,ENV (9),google.api_core.exception (0),







insert into `cienet-cmcs.amy_xlml_poc_prod.config_error` (err_code, err_regx, err_category) values 
('100', ".*tensorflow.python.framework.errors_impl.NotFoundError: {{function_node __wrapped__IteratorGetNext_output_types_1_device_.* No such file or directory .*Op:IteratorGetNext.* name.*",''),
('101', "airflow.exceptions.AirflowFailException: Bad pod phase: Failed",''),
('102', "airflow.exceptions.AirflowSensorTimeout: Sensor has timed out; run duration of .* seconds exceeds the specified timeout of .*",''),
('103', "AssertionError: Mantaray command failed with code 1",''),
('104', "AssertionError: XPK clean-up failed with code 1",''),
('105', "AssertionError: XPK command failed with code 1",''),
('106', "AttributeError: Unknown field for Operation: exception",''),
('107', "fabric.exceptions.GroupException: {<Connection host=.* <UnexpectedExit: cmd='.*?' exited=.*",''),
('108', "google.api_core.exceptions.NotFound: 404 GET https://storage.googleapis.com/download/storage/v1/b/ml-auto-solutions/o/.*?: No such object: .*?: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>)",''),
('109', "IndexError: list index out of range",''),
('110', "RuntimeError: Bad queued resource state FAILED",''),
('111', "subprocess.CalledProcessError: Command 'gcloud container node-pools (create|delete) .*' returned non-zero exit status 1.*",''),
('112', "TypeError: a bytes-like object is required, not 'str'",'');


insert into `cienet-cmcs.amy_xlml_poc_prod.config_error` (err_code, err_regx, err_category) values 
('100', ".*tensorflow.python.framework.errors_impl.NotFoundError: {{function_node __wrapped__IteratorGetNext_output_types_1_device_.* No such file or directory \[Op:IteratorGetNext\] name.*",''),
('', "airflow.exceptions.AirflowFailException: Bad pod phase: Failed",''),
('', "airflow\.exceptions\.AirflowSensorTimeout: Sensor has timed out; run duration of [\d\.]+ seconds exceeds the specified timeout of [\d\.]+\.",''),
('', "AssertionError: Mantaray command failed with code 1",''),
('', "AssertionError: XPK clean-up failed with code 1",''),
('', "AssertionError: XPK command failed with code 1",''),
('', "AttributeError: Unknown field for Operation: exception",''),
('', "fabric\.exceptions\.GroupException: {<Connection host=[\d\.]+>: <UnexpectedExit: cmd='.*?' exited=\d+>}",''),
('108', "google\.api_core\.exceptions\.NotFound: 404 GET https:\/\/storage\.googleapis\.com\/download\/storage\/v1\/b\/ml-auto-solutions\/o\/.*?: No such object: .*?: \('Request failed with status code', 404, 'Expected one of', <HTTPStatus\.OK: 200>, <HTTPStatus\.PARTIAL_CONTENT: 206>\)",''),
('', "IndexError: list index out of range",''),
('', "RuntimeError: Bad queued resource state FAILED",''),
('', "subprocess\.CalledProcessError: Command 'gcloud container node-pools (create|delete) .*' returned non-zero exit status 1\.",''),
('', "TypeError: a bytes-like object is required, not 'str'",'');



update `amy_xlml_poc_prod.config_error` 
set err_regx=
"(?i)google\\.api_core\\.exceptions\\.NotFound: 404 GET https:\\/\\/storage\\.googleapis\\.com\\/download\\/storage\\/v1\\/b\\/ml-auto-solutions\\/o\\/.*: No such object: .* \\('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>\\)"
where err_code='108';   

    

"google.api_core.exceptions.NotFound: 404 GET https://storage.googleapis.com/download/storage/v1/b/ml-auto-solutions/o/output%2Fpytorch_xla%2Ftorchbench%2Ftorchbench-all-nvidia-tesla-a100-2025-09-23-11-01-17%2Fmetric_report.jsonl?alt=media: No such object: ml-auto-solutions/output/pytorch_xla/torchbench/torchbench-all-nvidia-tesla-a100-2025-09-23-11-01-17/metric_report.jsonl: ('Request failed with status code', 404, 'Expected one of', <HTTPStatus.OK: 200>, <HTTPStatus.PARTIAL_CONTENT: 206>)"


