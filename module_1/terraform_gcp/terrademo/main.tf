terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.17.0"
    }
  }
}

provider "google" {
  project = "dataeng-448500"
  region  = "us-central1"
}

resource "google_storage_bucket" "demo-google_storage_bucket" {
  name          = "dataeng-448500-terra-bucket-demo"
  location      = "US"
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