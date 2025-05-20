provider "google-beta" {
  project = var.project_config.project_name
  region  = var.project_config.project_region
}

terraform {
  backend "gcs" {
    bucket = "${var.project_config.project_prefix}-composer-tfstate"
    prefix = "terraform/state"
  }
}

