# Meta Data Bucket for Lambda code and other project relevant files
resource "aws_s3_bucket" "meta_bucket" {
  bucket = "de-project-meta-bucket"
  force_destroy = true
}

# Ingestion Bucket for the raw data from the database
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "de-project-ingestion-bucket"
  force_destroy = true
}

# Processed bucket for the data post processing
resource "aws_s3_bucket" "processed_bucket" {
  bucket = "de-project-processed-bucket"
  force_destroy = true
}