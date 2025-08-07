# config.py
project_id = "cienet-cmcs"
dataset_id = "amy_xlml_poc_2"
bucket_name = "amy-xlml-poc"

# Destination for temporary GCS JSONL file
blob_path = "tmp/gke_cluster_info.jsonl"

# BigQuery settings
view_name = "cluster_view"
table_id = "gke_cluster_info_2"
table_id_metrics = "cluster_metrics"

# Script behavior settings
max_rows = 0 # Set <= 0 to fetch all rows; otherwise, limits the number of rows
external_dependencies = ["https://www.google.com", "https://api.example.com"]

