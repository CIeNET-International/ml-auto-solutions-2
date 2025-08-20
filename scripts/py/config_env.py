# config_env.py

CONFIG = {
    "cienet_prod": {
        "project_id": "cienet-cmcs",
        "dataset_id": "amy_xlml_poc_prod",
        "view_name": "dag_forward_schedule_simulation",
        "table_name": "dashboard_dag_forward_schedule_simulation"
    },
    "cienet_test": {
        "project_id": "cienet-cmcs",
        "dataset_id": "amy_xlml_poc_2",
        "view_name": "dag_forward_schedule_simulation",
        "table_name": "dashboard_dag_forward_schedule_simulation"
    },
    "staging": {
        "project_id": "my-staging-project",
        "dataset_id": "staging_dataset",
        "view_name": "my_view",
        "table_name": "my_table_persisted"
    },
    "prod": {
        "project_id": "my-prod-project",
        "dataset_id": "prod_dataset",
        "view_name": "my_view",
        "table_name": "my_table_persisted"
    }
}

