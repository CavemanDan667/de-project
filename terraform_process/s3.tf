# Processed bucket for the data post processing
resource "aws_s3_bucket" "processed_bucket" {
  bucket = "de-project-processed-bucket"
  force_destroy = true
}