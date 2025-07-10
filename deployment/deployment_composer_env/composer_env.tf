module "composer" {
  source = "../modules/composer_env"
  environment_name = var.environment_name
  region = var.region
  service_account = "ml-auto-solutions-dev@cienet-cmcs.iam.gserviceaccount.com"
}
