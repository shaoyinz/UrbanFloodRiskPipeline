resource "google_bigquery_dataset" "silver_ext" {
  dataset_id  = "${local.bq_dataset_prefix}_silver_ext"
  location    = var.data_location
  description = "External tables over silver-zone GeoParquet. Read-only from BQ; truth lives in GCS."

  delete_contents_on_destroy = false

  labels = merge(local.common_labels, { zone = "silver_ext" })

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "gold" {
  dataset_id  = "${local.bq_dataset_prefix}_gold"
  location    = var.data_location
  description = "Materialized dbt outputs: building_vulnerability fact, H3 aggregates."

  delete_contents_on_destroy = false

  labels = merge(local.common_labels, { zone = "gold" })

  depends_on = [google_project_service.bigquery]
}
