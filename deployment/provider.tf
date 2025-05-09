provider "google-beta" {
  project = var.project_config.project_name
  region  = var.project_config.project_region
}

terraform {
  backend "gcs" {
    bucket = "severus-composer-tfstate"
    prefix = "terraform/state"
  }
}

