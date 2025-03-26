variable "credentials" {
  description = "Path to the service account credentials file"
  default     = "./keys/my-creds.json"
}

variable "project" {
  description = "Project name"
  default     = "chrome-sandbox-448012-a5"
}

variable "region" {
  description = "Region"
  default     = "europe-west4-c"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "my_demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Bucket storage class"
  default     = "chrome-sandbox-448012-a5-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}