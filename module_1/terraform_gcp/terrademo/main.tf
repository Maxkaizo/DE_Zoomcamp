terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "hw3_dataeng" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Crear un dataset de BigQuery
resource "google_bigquery_dataset" "hw3_dataeng" {
  dataset_id    = var.bq_dataset_name # Nombre del dataset
  location      = var.location        # Ubicación del dataset (por ejemplo, "US" o "EU")
  friendly_name = "Dataset de Ejemplo"
  description   = "Un dataset de ejemplo creado con Terraform"
}