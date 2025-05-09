bigquery_datasets = [
  {
    id        = "severus_benchmark_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "severus_xlml_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "severus_dev_benchmark_dataset"
    location  = "US"
    env_stage = "dev"
  },
  {
    id        = "severus_dev_xlml_dataset"
    location  = "US"
    env_stage = "dev"
  }
]

bigquery_tables = [
  {
    dataset_id     = "severus_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "severus_dev_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "severus_dev_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "severus_dev_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "severus_dev_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "severus_dev_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "severus_dev_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  }
]
