############################### CREATES S3 BUCKETS ###############################
# Ingestion Bucket for the raw data from the database
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "de-project-ingestion-bucket"
  force_destroy = true
}
# Transform Bucket stores parquet files from transform data
resource "aws_s3_bucket" "transformed_bucket" {
  bucket = "de-project-transformed-bucket"
  force_destroy = true
}

