bigquery_datasets = [
  {
    id        = "${var.project_config.project_prefix}_benchmark_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "${var.project_config.project_prefix}_xlml_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "${var.project_config.project_prefix}_dev_benchmark_dataset"
    location  = "US"
    env_stage = "dev"
  },
  {
    id        = "${var.project_config.project_prefix}_dev_xlml_dataset"
    location  = "US"
    env_stage = "dev"
  }
]

bigquery_tables = [
  {
    dataset_id     = "${var.project_config.project_prefix}_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "${var.project_config.project_prefix}_dev_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  }
]