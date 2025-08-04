CREATE TABLE `amy_xlml_poc_2`.`dag` (
 `dag_id` STRING NOT NULL,
  `root_dag_id` STRING,
  `is_paused` BOOL,
  `is_subdag` BOOL,
  `is_active` BOOL,
  `last_parsed_time` TIMESTAMP,
  `last_pickled` TIMESTAMP,
  `last_expired` TIMESTAMP,
  `scheduler_lock` BOOL,
  `pickle_id` INT64,
  `fileloc` STRING,
  `processor_subdir` STRING,
  `owners` STRING,
  `dag_display_name` STRING,
  `description` STRING,
  `default_view` STRING,
  `schedule_interval` STRING,
  `timetable_description` STRING,
  `dataset_expression` STRING,
  `max_active_tasks` INT64 NOT NULL,
  `max_active_runs` INT64,
  `max_consecutive_failed_dag_runs` INT64 NOT NULL,
  `has_task_concurrency_limits` BOOL NOT NULL,
  `has_import_errors` BOOL,
  `next_dagrun` TIMESTAMP,
  `next_dagrun_data_interval_start` TIMESTAMP,
  `next_dagrun_data_interval_end` TIMESTAMP,
  `next_dagrun_create_after` TIMESTAMP
);

CREATE TABLE `amy_xlml_poc_2`.`dag_run` (
 `id` INT64 NOT NULL,
  `dag_id` STRING NOT NULL,
  `queued_at` TIMESTAMP,
  `execution_date` TIMESTAMP NOT NULL,
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP,
  `state` STRING,
  `run_id` STRING NOT NULL,
  `creating_job_id` INT64,
  `external_trigger` BOOL,
  `run_type` STRING NOT NULL,
  `conf` BYTES,
  `data_interval_start` TIMESTAMP,
  `data_interval_end` TIMESTAMP,
  `last_scheduling_decision` TIMESTAMP,
  `dag_hash` STRING,
  `log_template_id` INT64,
  `updated_at` TIMESTAMP,
  `clear_number` INT64 NOT NULL
);


CREATE TABLE `amy_xlml_poc_2`.`dag_tag` (
  `name` STRING NOT NULL,
  `dag_id` STRING NOT NULL
);

CREATE TABLE `amy_xlml_poc_2`.`task_fail` (
  `id` INT64 NOT NULL,
  `task_id` STRING NOT NULL,
  `dag_id` STRING NOT NULL,
  `run_id` STRING NOT NULL,
  `map_index` INT64 NOT NULL,
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP,
  `duration` INT64
);

CREATE TABLE `amy_xlml_poc_2`.`task_instance` (
  `task_id` STRING NOT NULL,
  `dag_id` STRING NOT NULL,
  `run_id` STRING NOT NULL,
  `map_index` INT64 NOT NULL,
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP,
  `duration` FLOAT64,
  `state` STRING,
  `try_number` INT64,
  `max_tries` INT64,
  `hostname` STRING,
  `unixname` STRING,
  `job_id` INT64,
  `pool` STRING NOT NULL,
  `pool_slots` INT64 NOT NULL,
  `queue` STRING,
  `priority_weight` INT64,
  `operator` STRING,
  `custom_operator_name` STRING,
  `queued_dttm` TIMESTAMP,
  `queued_by_job_id` INT64,
  `pid` INT64,
  `executor` STRING,
  `executor_config` BYTES,
  `updated_at` TIMESTAMP,
  `rendered_map_index` STRING,
  `external_executor_id` STRING,
  `trigger_id` INT64,
  `trigger_timeout` TIMESTAMP,
  `next_method` STRING,
  `next_kwargs` STRING,
  `task_display_name` STRING
);

CREATE TABLE `amy_xlml_poc_2`.`task_instance_history` (
  `id` INT64 NOT NULL,
  `task_id` STRING NOT NULL,
  `dag_id` STRING NOT NULL,
  `run_id` STRING NOT NULL,
  `map_index` INT64 NOT NULL,
  `try_number` INT64 NOT NULL,
  `start_date` TIMESTAMP,
  `end_date` TIMESTAMP,
  `duration` FLOAT64,
  `state` STRING,
  `max_tries` INT64,
  `hostname` STRING,
  `unixname` STRING,
  `job_id` INT64,
  `pool` STRING NOT NULL,
  `pool_slots` INT64 NOT NULL,
  `queue` STRING,
  `priority_weight` INT64,
  `operator` STRING,
  `custom_operator_name` STRING,
  `queued_dttm` TIMESTAMP,
  `queued_by_job_id` INT64,
  `pid` INT64,
  `executor` STRING,
  `executor_config` BYTES,
  `updated_at` TIMESTAMP,
  `rendered_map_index` STRING,
  `external_executor_id` STRING,
  `trigger_id` INT64,
  `trigger_timeout` TIMESTAMP,
  `next_method` STRING,
  `next_kwargs` STRING,
  `task_display_name` STRING
);

CREATE TABLE `amy_xlml_poc_2`.`rendered_task_instance_fields` (
  `dag_id` STRING NOT NULL,
  `task_id` STRING NOT NULL,
  `run_id` STRING NOT NULL,
  `map_index` INT64 NOT NULL,
  `rendered_fields` STRING NOT NULL,
  `k8s_pod_yaml` STRING
);

CREATE TABLE `amy_xlml_poc_2`.`serialized_dag` (
  `dag_id` STRING NOT NULL,
  `fileloc` STRING NOT NULL,
  `fileloc_hash` INT64 NOT NULL,
  `data` STRING,
  `data_compressed` BYTES,
  `last_updated` TIMESTAMP NOT NULL,
  `dag_hash` STRING NOT NULL,
  `processor_subdir` STRING
);



