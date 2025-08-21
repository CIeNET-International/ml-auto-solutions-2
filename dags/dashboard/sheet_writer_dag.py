from __future__ import annotations

import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.google.suite.transfers.gcs_to_sheets import GCSToGoogleSheetsOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


# 設定你的變數
# SPREADSHEET_ID = '1Y7ed1imMLuTRuWSkM_CJSqv8tL2jrhxsRxzaaI6LvRs'
# WORKSHEET_NAME = 'test1'
SPREADSHEET_ID = '1FkyzvC0HGxFPGjIpCV4pWHB_QFWUOtLPIlDfnBNvt4Q'
WORKSHEET_NAME = 'unhealthy_clusters!A1'


@task
def append_to_sheet():
  hook = GSheetsHook(gcp_conn_id="google_cloud_default")
  """
  [
  IssueType | Project ID | Cluster Name | Cluster Status | Cluster Status Message | Node Pool Name | Node Pool Status | Node 
  Pool Status Message | Appended At
  ]
  """
  values = [
    ["NodePool", "cloud-tpu-multipod-dev", "v4-128-bodaborg-us-central2-b", "RUNNING", "",
     "v4-128-bodaborg-us-central2-b-np-3", "ERROR", "Something Wrrong", "2025-08-21T07:31:17.058472+00:00"],
    ["Cluster", "tpu-prod-env-large-adhoc", "bodaborg-v6e-256", "NOT EXIST", "Cluster not found in GKE API.", "", "",
     "", "2025-08-21T07:31:43.083588+00:00"]
  ]
  hook.append_values(
    spreadsheet_id=SPREADSHEET_ID,
    range_=WORKSHEET_NAME,
    values=values,
    insert_data_option="INSERT_ROWS",
    value_input_option="RAW",  # "RAW" or "USER_ENTERED"
  )


with DAG(
    "append_gsheet_records",
    start_date=datetime.datetime(2025, 8, 13),
    schedule_interval=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
  append_task = append_to_sheet()