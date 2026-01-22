variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "project" {
  description = "Project"
  default     = "terraform-test-485011"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "dq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "terraform-test-485011-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}