resource "google_artifact_registry_repository" "private-xlml-index" {
  project       = var.project_config.project_name
  location      = var.project_config.project_region
  repository_id = "chengken-xlml-private" # Suggested Naming: <prefix>-xlml-private
  description   = "Packaged `xlml` wheels"
  format        = "python"
}
