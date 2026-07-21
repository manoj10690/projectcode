terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# 1. Enable required APIs  (Ch.5.1.4 Step 1-2: terraform init / apply)
# ---------------------------------------------------------------------------
locals {
  services = [
    "run.googleapis.com",
    "dataflow.googleapis.com",
    "sqladmin.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "workflows.googleapis.com",
    "workflowexecutions.googleapis.com",
    "cloudkms.googleapis.com",
    "eventarc.googleapis.com",
    "pubsub.googleapis.com",
    "secretmanager.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "composer.googleapis.com",
    "aiplatform.googleapis.com",
    "servicenetworking.googleapis.com",
    "iam.googleapis.com"
  ]
}

resource "google_project_service" "apis" {
  for_each           = toset(local.services)
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ---------------------------------------------------------------------------
# 2. KMS  (Ch.5.1.4 Step 3: envelope encryption key ring)
# ---------------------------------------------------------------------------
resource "google_kms_key_ring" "ingest_keyring" {
  name       = "${var.env}-ingest-keyring"
  location   = var.region
  depends_on = [google_project_service.apis]
}

resource "google_kms_crypto_key" "ingest_cmek" {
  name            = "${var.env}-ingest-cmek"
  key_ring        = google_kms_key_ring.ingest_keyring.id
  rotation_period = "7776000s" # 90 days
  purpose         = "ENCRYPT_DECRYPT"
}

data "google_project" "project" {}

resource "google_kms_crypto_key_iam_member" "gcs_sa_binding" {
  crypto_key_id = google_kms_crypto_key.ingest_cmek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:service-${data.google_project.project.number}@gs-project-accounts.iam.gserviceaccount.com"
}

resource "google_kms_crypto_key_iam_member" "bq_sa_binding" {
  crypto_key_id = google_kms_crypto_key.ingest_cmek.id
  role          = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
  member        = "serviceAccount:bq-${data.google_project.project.number}@bigquery-encryption.iam.gserviceaccount.com"
}

# ---------------------------------------------------------------------------
# 3. Storage: Bronze landing bucket + Quarantine (DLQ) bucket
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "bronze" {
  name                        = "${var.project_id}-${var.env}-bronze"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = var.env != "prod"

  encryption {
    default_kms_key_name = google_kms_crypto_key.ingest_cmek.id
  }

  lifecycle_rule {
    condition { age = 90 }
    action { type = "SetStorageClass" storage_class = "COLDLINE" }
  }

  depends_on = [google_kms_crypto_key_iam_member.gcs_sa_binding]
}

resource "google_storage_bucket" "dlq" {
  name                        = "${var.project_id}-${var.env}-quarantine-dlq"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = var.env != "prod"

  encryption {
    default_kms_key_name = google_kms_crypto_key.ingest_cmek.id
  }

  depends_on = [google_kms_crypto_key_iam_member.gcs_sa_binding]
}

resource "google_storage_bucket" "dataflow_staging" {
  name                        = "${var.project_id}-${var.env}-df-staging"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true
}

# ---------------------------------------------------------------------------
# 4. Cloud SQL (Postgres) -- Metadata Store: pipelines / schema_ledger / dq_rules
# ---------------------------------------------------------------------------
resource "google_sql_database_instance" "metadata_db" {
  name             = "${var.env}-ingest-metadata"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier              = var.env == "prod" ? "db-custom-2-7680" : "db-f1-micro"
    availability_type = var.env == "prod" ? "REGIONAL" : "ZONAL"

    disk_size       = 50
    disk_autoresize = true
    disk_type       = "PD_SSD"

    backup_configuration {
      enabled    = true
      start_time = "02:00"
    }

    ip_configuration {
      ipv4_enabled = true
      dynamic "authorized_networks" {
        for_each = var.env == "dev" ? [1] : []
        content {
          name  = "allow-all-dev-only"
          value = "0.0.0.0/0"
        }
      }
    }
  }

  deletion_protection = var.env == "prod"
  depends_on           = [google_project_service.apis]
}

resource "google_sql_database" "metadata" {
  name     = "ingest_metadata"
  instance = google_sql_database_instance.metadata_db.name
}

resource "google_sql_user" "app_user" {
  name     = "ingest_app"
  instance = google_sql_database_instance.metadata_db.name
  password = var.db_password
}

resource "google_secret_manager_secret" "db_password" {
  secret_id = "${var.env}-ingest-db-password"
  replication {
    auto {}
  }
  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "db_password_v1" {
  secret      = google_secret_manager_secret.db_password.id
  secret_data = var.db_password
}

# ---------------------------------------------------------------------------
# 5. BigQuery datasets -- Silver / Gold (Bronze is primarily GCS + external tables)
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "bronze" {
  dataset_id = "bronze"
  location   = var.region
}

resource "google_bigquery_dataset" "silver" {
  dataset_id = "silver"
  location   = var.region
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = "gold"
  location   = var.region
}

# ---------------------------------------------------------------------------
# 6. Service Accounts (least privilege, Ch.4.5 Identity Guardrails)
# ---------------------------------------------------------------------------
resource "google_service_account" "ui_sa" {
  account_id   = "${var.env}-ingest-ui"
  display_name = "Self-service UI (Cloud Run) service account"
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "${var.env}-ingest-dataflow"
  display_name = "Dataflow metadata-driven engine service account"
}

resource "google_service_account" "orchestrator_sa" {
  account_id   = "${var.env}-ingest-orchestrator"
  display_name = "Cloud Workflows / Composer orchestrator service account"
}

resource "google_project_iam_member" "ui_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.ui_sa.email}"
}

resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "dataflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_project_iam_member" "orchestrator_dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.orchestrator_sa.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_bronze_writer" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "dataflow_dlq_writer" {
  bucket = google_storage_bucket.dlq.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

# ---------------------------------------------------------------------------
# 7. Cloud Run -- self-service UI (Ch.3.3 "UI: Google Cloud Run")
# ---------------------------------------------------------------------------
resource "google_cloud_run_v2_service" "ingest_ui" {
  name     = "${var.env}-ingest-ui"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    service_account = google_service_account.ui_sa.email

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }

    containers {
      image = "gcr.io/${var.project_id}/ingest-ui:latest" # build & push via ui/Dockerfile first
      resources {
        limits = {
          cpu    = "2"
          memory = "4Gi"
        }
      }
      env {
        name  = "DB_INSTANCE_CONNECTION_NAME"
        value = google_sql_database_instance.metadata_db.connection_name
      }
      env {
        name  = "DB_NAME"
        value = google_sql_database.metadata.name
      }
      env {
        name  = "DB_USER"
        value = google_sql_user.app_user.name
      }
      env {
        name = "DB_PASSWORD"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.db_password.secret_id
            version = "latest"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_service_iam_member" "public_invoker" {
  count    = var.env == "dev" ? 1 : 0
  name     = google_cloud_run_v2_service.ingest_ui.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# ---------------------------------------------------------------------------
# 8. Cloud Workflows -- prototype orchestrator (Ch.5.1.1)
# ---------------------------------------------------------------------------
resource "google_workflows_workflow" "ingest_workflow" {
  name            = "${var.env}-ingest-workflow"
  region          = var.region
  service_account = google_service_account.orchestrator_sa.id
  source_contents = file("${path.module}/../orchestration/workflows.yaml")
  depends_on      = [google_project_service.apis]
}

# ---------------------------------------------------------------------------
# 9. Eventarc trigger: new file in Bronze bucket -> Cloud Workflows execution
# ---------------------------------------------------------------------------
resource "google_eventarc_trigger" "bronze_file_trigger" {
  name     = "${var.env}-bronze-file-trigger"
  location = var.region

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }
  matching_criteria {
    attribute = "bucket"
    value     = google_storage_bucket.bronze.name
  }

  destination {
    workflow = google_workflows_workflow.ingest_workflow.id
  }

  service_account = google_service_account.orchestrator_sa.email
  depends_on      = [google_project_service.apis]
}
