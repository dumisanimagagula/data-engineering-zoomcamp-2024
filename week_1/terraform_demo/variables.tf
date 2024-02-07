variable "credentials" {
    description = "My GCP credentials file path"
    default = "./keys/credentials.json"
}

variable "project_id" {
    description = "This is my Project ID"
    default = "terraform-demo-412722"
}

variable "region" {
    description = "Project region"
    default = "us-central1"
}

variable "location" {
    description = "Project location"
    default = "US"
}

variable "bd_dataset_name" {
    description = "My BigQuery dataset name"
    default = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "My Storage Bucket name"
    default = "terraform-demo-412722-terraform-bucket"
}

variable "gcs_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}