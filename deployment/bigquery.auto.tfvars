# Suggested to add prefix in front of each id, ex: severus_benchmark_dataset
bigquery_datasets = [
  {
    id        = "chengken_benchmark_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "chengken_xlml_dataset"
    location  = "US"
    env_stage = "prod"
  },
  {
    id        = "chengken_dev_benchmark_dataset"
    location  = "US"
    env_stage = "dev"
  },
  {
    id        = "chengken_dev_xlml_dataset"
    location  = "US"
    env_stage = "dev"
  }
]
# Suggested to add prefix in front of each dataset_id
bigquery_tables = [
  {
    dataset_id     = "chengken_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "prod"
  },
  {
    dataset_id     = "chengken_dev_benchmark_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "chengken_dev_benchmark_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "chengken_dev_benchmark_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "chengken_dev_xlml_dataset"
    table_id       = "job_history"
    schema_id      = "schema/job_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "chengken_dev_xlml_dataset"
    table_id       = "metric_history"
    schema_id      = "schema/metric_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  },
  {
    dataset_id     = "chengken_dev_xlml_dataset"
    table_id       = "metadata_history"
    schema_id      = "schema/metadata_history.json"
    partition_type = "MONTH"
    env_stage      = "dev"
  }
]
