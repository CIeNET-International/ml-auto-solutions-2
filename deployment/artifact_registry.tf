resource "google_artifact_registry_repository" "private-xlml-index" {
  project       = var.project_config.project_name
  location      = "us-east5"
  repository_id = "severus-xlml-private"
  description   = "Packaged `xlml` wheels"
  format        = "python"
}
