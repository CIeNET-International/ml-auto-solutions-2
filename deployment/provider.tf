provider "google-beta" {
  project = var.project_config.project_name
  region  = var.project_config.project_region
}

terraform {
  backend "gcs" {
    bucket = "chengken-composer-ml-auto-solutions-tfstate" # Suggested Naming: <prefix>-ml-tfstate
    prefix = "terraform/state"
  }
}

