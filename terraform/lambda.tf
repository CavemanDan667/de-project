resource "aws_lambda_function" "ingestion_lambda" {
  filename      = "./zipped/demo.zip"
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingestion_lambda_role.arn
  runtime       = "python3.11"
  handler = "demo.handler"
}