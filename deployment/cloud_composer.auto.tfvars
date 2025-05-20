project_config = {
  project_name   = "cienet-cmcs"
  project_number = "562977990677"
  project_region = "us-east5"
  project_prefix = "severus"
}

environment_config = [
  {
    environment_name   = "${var.project_config.project_prefix}-ml-solutions"
    service_account_id = "${var.project_config.project_prefix}-ml-sa"
  }
]
