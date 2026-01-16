from airflow.plugins_manager import AirflowPlugin
from github_issue_listener import GithubIssueListener
from google_sheet_listener import GoogleSheetListener


class ListenerPlugins(AirflowPlugin):
  name = "listener_plugins"
  # listeners = [GithubIssueListener(), GoogleSheetListener()]
  listeners = [GoogleSheetListener()]
