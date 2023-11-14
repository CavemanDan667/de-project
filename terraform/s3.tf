############################### CREATES S3 BUCKETS ###############################
# Ingestion Bucket for the raw data from the database
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "de-project-ingestion-bucket"
  force_destroy = true
}
# Process Bucket stores parquet files from transform data
resource "aws_s3_bucket" "processed_bucket" {
  bucket = "de-project-processed-bucket"
  force_destroy = true
}

