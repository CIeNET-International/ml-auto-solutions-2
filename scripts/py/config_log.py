# BigQuery config
BQ_PROJECT_ID = "cienet-cmcs"
BQ_DATASET = "amy_xlml_poc_prod"
BQ_VIEW_NAME = "latest_dag_runs"
BQ_VIEW_NAME_NO_CLUSTER = "last_one_run_wo_cluster"
BQ_DEST_TABLE = "dag_runs_with_logs"
BQ_DEST_TABLE_CLUSTER_FROM_LOG = "cluster_info_from_log"


#Logs REGX



# Logs config
LOGS_PROJECT_ID = "cloud-ml-auto-solutions"
RESOURCE_TYPE = "cloud_composer_workflow"
COMPOSER_ENVIRONMENT_NAME = "ml-automation-solutions"

# Log Explorer UI host
#LOG_EXPLORER_HOST = "https://console.cloud.google.com/logs/query"
LOG_EXPLORER_HOST = "https://pantheon.corp.google.com/logs/query"

# Optional: default time padding for URLs (seconds)
LOG_TIME_PADDING = 0

# Extend query end time range by this many seconds
LOG_QUERY_END_PADDING_SECONDS = 5  

LOG_QUERY_SEVERITY = "ERROR"

GCS_PROJECT_ID = "cienet-cmcs"
GCS_BUCKET_NAME = "amy-xlml-poc-prod"

DAGS_TO_QUERY_LOGS = [
    #"jax_ai_image_candidate_tpu_e2e",
    #"maxtext_muti_tier_p2_checkpointing",
    #"new_internal_stable_release_a3ultra_llama3.1-405b_256gpus_fp8_maxtext",
    #"maxstar_v5e_llama2_70b_daily",
    #"maxtext_gpu_end_to_end",
]

# ----------------------------
# HACKED TIME CONFIG (by dag_id) --for test env only
# ----------------------------
HACKED_DAG_TIMES = {
#    "maxtext_muti_tier_p2_checkpointing": {
#        "run": {
#            "execution": "2025-08-13T10:00:00Z",
#            "start": "2025-08-14T10:00:02Z",
#            "end": "2025-08-14T11:48:01Z",
#            "run_id": "scheduled__2025-08-13T10:00:00+00:00"
#        },
#        "tests": {
#            "maxtext-multi-tier-checkpointing-p2-save-3xv6e-16": {
#                "start": "2025-08-14T10:02:16Z",
#                "end": "2025-08-14T10:40:32Z"
#            },
#            "maxtext-multi-tier-checkpointing-p2-emulate-disruption-v6e-16": {
#                "start": "2025-08-14T10:41:19Z",
#                "end": "2025-08-14T11:16:11Z"
#            },
#            "maxtext-multi-tier-checkpointing-p2-restore-3xv6e-16": {
#                "start": "2025-08-14T11:16:54Z",
#                "end": "2025-08-14T11:47:58Z"
#            },
#        }
#    }
}
