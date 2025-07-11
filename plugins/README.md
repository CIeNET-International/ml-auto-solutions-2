The "on_failure_actions" plugin listens to the DAGs run results and takes action for failed DAG runs. Currently, the action is to file a GitHub issue for the failed DAG run.

## Pre-requisites:
To leverage the "on_failure_actions" plugin, ensure the following conditions are met:

### 1.  **DAG Opt-In:**
Each DAG intended to utilize this feature **must include the `"on_failure_alert"` tag** within its DAG definition. DAGs without this specific tag will be ignored by the plugin's failure-handling logic, and no GitHub issue will be filed for their failures.

    with DAG(
        dag_id='my_critical_dag',
        # ... other DAG parameters ...
        tags=['data_ingestion', 'critical', 'on_failure_alert'], # <--- Add this tag
    ) as dag:
        # ... tasks ...

### 2.  **GitHub Owner Mapping:**
For accurate issue assignment, ensure that the `owner` property defined for tests within your DAGs corresponds directly to valid **GitHub usernames**. The plugin will collect unique test owners from the failed DAG and attempt to assign the GitHub issue to these users.

#### Example task definition
    my_task = BashOperator(
        task_id='process_data',
        bash_command='...',
        owner='github_username_here', # This should be a valid GitHub username
    )

    # Or

    @task(owner='github_username_here') # This should be a valid GitHub username
    def task_a():
        pass
### 2.5 **GitHub Owner for test config:**
If you are using test_config.py in your DAG, you should fill test_owner attribute with your github username

#### Example test definition
    maxtext_v4_configs_test = gke_config.get_gke_config(
        num_slices=slice_num,
        cluster=clusters[accelerator],
        time_out_in_min=60,
        test_name=f"maxtext-checkpointing-{mode.value}-{chkpt_mode}",
        run_model_cmds=command,
        docker_image=image.value,
        test_owner='github_username_here',  # This should be a valid GitHub username
    ).run()

## Configuration and Installation:
### Set up via GCP console UI
1. From GCP console UI, Your Composer Env -> Tab -> Pypi packages -> Edit -> Add 'apache-airflow-providers-github' -> Save

2. From GCP console UI, search for "Secret Manager", and add conn_id 'github_default' into Secret Manager. For the management of Github token, please refer to [Github access tokens]
   1. key: airflow-connections-<composer_environment_name>-github_app
      secret value:
      {
           "app_id": "<APP_ID>",
           "installation_id": "<INSTALLATION_ID>",
           "private_key": "<PRIVATE_KEY>"
      }

3. Composer -> Airflow configuration overrides -> Edit, reference: https://cloud.google.com/composer/docs/composer-1/configure-secret-manager
   1. secrets | backend | airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend

4. Upload 'on_failure_actions.py' to \<DAG Bucket\>/plugins/

### Set up via Google Cloud CLI
1. Set the environment variables in your terminal:
    ```
    COMPOSER_ENVIRONMENT_NAME=<Composer Environment Name>
    COMPOSER_LOCATION=<Composer Location>
    COMPOSER_DAG_BUCKET=gs://<DAG Bucket>
    APP_ID=<GitHub App Id>
    INSTALLATION_ID=<GitHub Installation Id>
    PRIVATE_KEY=<GitHub App Private Key>
    ```

2. Add `apache-airflow-providers-github` Pypi package to your Composer environment.
    ```
    gcloud composer environments update ${COMPOSER_ENVIRONMENT_NAME} --location=${COMPOSER_LOCATION} --update-pypi-package='apache-airflow-providers-github'
    ```

3. Add the connection conn_id `github_default` to Secret Manager. This command creates a secret for accessing Github.
    ```
    echo "{\"app_id\": \"${APP_ID}\", \"installation_id\": \"${INSTALLATION_ID}\", \"private_key\": \"${PRIVATE_KEY}\"}" | gcloud secrets create airflow-connections-${COMPOSER_ENVIRONMENT_NAME}-github_app --data-file=-
    ```

4. Upload Composer plugins to the folder for dags in your Composer bucket
    ```
    gcloud storage cp ./on_failure_actions.py "${COMPOSER_DAG_BUCKET}/plugins/"
    ```


