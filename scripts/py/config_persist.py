PERSIST_PAIR = {
    'base': {
        'view_name': 'base_view',
        'table_name': 'base'
    },
    'statistic': {
        'view_name': 'statistic_last_window_view',
        'table_name': 'statistic_last_window'
    },
    'dag': {
        'view_name': 'all_dag_view',
        'table_name': 'all_dag'
    },

    'profile': {
        'view_name': 'dag_execution_profile_with_cluster_view',
        'table_name': 'dag_execution_profile_with_cluster'
    },
    'sch': {
        'view_name': 'dag_forward_schedule_simulation',
        'table_name': 'dashboard_dag_forward_schedule_simulation'
    },
    'global': {
        'view_name': 'global_dag_summary_view',
        'table_name': 'global_dag_summary'
    },
}

# Environment-specific settings
ENV = {
    'dev': {
        'project_id': 'cienet-cmcs',
        'dataset_id': 'amy_xlml_poc_prod'
    },
    'prod': {
        'project_id': 'prod-project-456',
        'dataset_id': 'prod_dataset'
    }
}

