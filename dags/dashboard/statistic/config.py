



# Common Config

# BigQuery config
BQ_PROJECT_ID = "cienet-cmcs"
BQ_DATASET = "amy_xlml_poc_prod"

#GCS
GCS_PROJECT_ID = "cienet-cmcs"
GCS_BUCKET_NAME = "amy-xlml-poc-prod"


#PERSIST
PERSIST_PAIR = {
    'all_dag_base': {
        'view_name': 'all_dag_base_view',
        'table_name': 'all_dag_base'
    },
    'base': {
        'view_name': 'base_view',
        'table_name': 'base'
    },
    'dag_duration': {
        'view_name': 'dag_duration_stat_view',
        'table_name': 'dag_duration_stat'
    },
    'test_duration': {
        'view_name': 'dag_test_duration_stat_view',
        'table_name': 'dag_test_duration_stat'
    },
    'statistic_data': {
        'view_name': 'statistic_data_view',
        'table_name': 'statistic_data'
    },
    'statistic': {
        'view_name': 'statistic_last_window_view',
        'table_name': 'statistic_last_window'
    },
    'dag_cluster_map': {
        'view_name': 'dag_cluster_map_view',
        'table_name': 'dag_cluster_map'
    },
    'run_status': {
        'view_name': 'dag_run_status_base_view',
        'table_name': 'dag_run_status_base'
    },
    'dag': {
        'view_name': 'all_dag_view',
        'table_name': 'all_dag'
    },
    'test': {
        'view_name': 'all_test_view',
        'table_name': 'all_test'
    },
    'profile': {
        'view_name': 'dag_execution_profile_with_cluster_view',
        'table_name': 'dag_execution_profile_with_cluster'
    },
    'sch': {
        'view_name': 'dag_forward_schedule_simulation',
        'table_name': 'dashboard_dag_forward_schedule_simulation'
    },
    'gke': {
        'view_name': 'gke_cluster_info_view',
        'table_name': 'dashboard_gke_cluster_info_view'
    },
    'global': {
        'view_name': 'global_dag_summary_view',
        'table_name': 'global_dag_summary'
    },
}

#LOGS
LOG_PROJECT_ID = "cloud-ml-auto-solutions"
LOG_RESOURCE_TYPE = "cloud_composer_workflow"
LOG_COMPOSER_ENVIRONMENT_NAME = "ml-automation-solutions"

# Log Explorer UI host
#LOG_EXPLORER_HOST = "https://console.cloud.google.com/logs/query"
LOG_EXPLORER_HOST = "https://pantheon.corp.google.com/logs/query"

# Optional: default time padding for URLs (seconds)
LOG_TIME_PADDING = 0

# Extend query end time range by this many seconds
LOG_QUERY_END_PADDING_SECONDS = 5  

LOG_QUERY_SEVERITY = "ERROR"

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

