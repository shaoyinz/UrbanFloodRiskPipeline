output "raw_bucket" {
  description = "gs:// URL of the raw zone bucket."
  value       = google_storage_bucket.raw.url
}

output "silver_bucket" {
  description = "gs:// URL of the silver zone bucket."
  value       = google_storage_bucket.silver.url
}

output "gold_bucket" {
  description = "gs:// URL of the gold zone bucket."
  value       = google_storage_bucket.gold.url
}

output "bq_silver_ext_dataset" {
  description = "Fully-qualified ID of the silver external-tables dataset."
  value       = "${var.project_id}.${google_bigquery_dataset.silver_ext.dataset_id}"
}

output "bq_gold_dataset" {
  description = "Fully-qualified ID of the gold (dbt outputs) dataset."
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}"
}
