1. add conn_id 'github_default' into Secret Manager. Prefix is namespace for separating different users.
   1. key: airflow-connections-<prefix>-github_default 
   2. value: 
       {
           "conn_type": "http",
           "host": "https://api.github.com",
           "password": "\<Personal Access Token\>"
       }
2. add variable 'github_listener_config' into Secret Manager.
   1. key: airflow-variables-<prefix>-github_listener_config
   2. value:
       {
           "enabled": bool,
           "full_repo_name": "\<owner or org\>/\<repo_name\>"
       }
3. Composer -> Airflow configuration overrides -> Edit
   1. secrets | backend | airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
   2. secrets | backends_order | custom,environment_variable,metastore
   3. secrets | backend_kwargs | {"project_id": "\<project_id\>"}
4. Modify 'prefix' variable in plugins source code and upload to \<Bucket\>/plugins/
5. (Optional) Add or modify a variable to force restart scheduler