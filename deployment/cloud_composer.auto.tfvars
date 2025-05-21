project_config = {
  project_name   = "cienet-cmcs"
  project_number = "562977990677"
  project_region = "us-east5"
}

environment_config = [
  {
    environment_name   = "ml-automation-solutions" # Suggested Naming: <prefix>-ml-dev
    service_account_id = "ml-auto-solutions-sa" # Suggested Naming: <prefix>-ml-sa
  }
]
