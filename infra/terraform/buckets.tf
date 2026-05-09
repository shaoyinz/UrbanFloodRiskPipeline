resource "google_storage_bucket" "raw" {
  name          = "${local.bucket_prefix}-raw"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  labels = merge(local.common_labels, { zone = "raw" })
}

resource "google_storage_bucket" "silver" {
  name          = "${local.bucket_prefix}-silver"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = merge(local.common_labels, { zone = "silver" })
}

resource "google_storage_bucket" "gold" {
  name          = "${local.bucket_prefix}-gold"
  location      = var.data_location
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  labels = merge(local.common_labels, { zone = "gold" })
}
