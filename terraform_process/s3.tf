# Meta Data Bucket for Lambda code and other project relevant files
resource "aws_s3_bucket" "test_bucket" {
  bucket = "test-bucket-de-project"
  force_destroy = true
}