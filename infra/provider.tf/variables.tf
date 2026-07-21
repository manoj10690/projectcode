variable "project_id" {
  description = "GCP project ID (e.g. dev-ingest-123456)"
  type        = string
}

variable "region" {
  description = "Primary GCP region"
  type        = string
  default     = "us-central1"
}

variable "env" {
  description = "Environment name: dev | staging | prod"
  type        = string
  default     = "dev"
}

variable "db_password" {
  description = "Master password for the metadata Cloud SQL instance"
  type        = string
  sensitive   = true
}

variable "alert_email" {
  description = "Email to receive pipeline failure / DLQ threshold alerts"
  type        = string
  default     = ""
}
