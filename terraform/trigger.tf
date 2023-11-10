resource "aws_lambda_permission" "allow_s3_to_invoke_process_lambda" {
    statement_id = "AllowExecutionFromS3Bucket"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.process_lambda.function_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.ingestion_bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_ingestion_notification" {
  bucket = "de-project-ingestion-bucket"

  lambda_function {
    lambda_function_arn = aws_lambda_function.process_lambda.arn
    events = [ "s3:ObjectCreated:*" ]
  }
}

resource "aws_lambda_permission" "allow_s3_to_invoke_load_lambda" {
    statement_id = "AllowExecutionFromS3Bucket"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.load_lambda.function_name
    principal = "s3.amazonaws.com"
    source_arn = aws_s3_bucket.processed_bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_processed_notification" {
  bucket = "de-project-processed-bucket"

  lambda_function {
    lambda_function_arn = aws_lambda_function.load_lambda.arn
    events = [ "s3:ObjectCreated:*" ]
  }
}