output "bronze_bucket" {
  value = google_storage_bucket.bronze.name
}

output "dlq_bucket" {
  value = google_storage_bucket.dlq.name
}

output "metadata_db_connection_name" {
  value = google_sql_database_instance.metadata_db.connection_name
}

output "ui_url" {
  value = google_cloud_run_v2_service.ingest_ui.uri
}

output "dataflow_service_account" {
  value = google_service_account.dataflow_sa.email
}
