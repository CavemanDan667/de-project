resource "aws_lambda_function" "ingestion_lambda" {
  filename      = "./zipped/ingestion.zip"
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingestion_lambda_role.arn
  runtime       = "python3.11"
  handler = "ingestion.handler"
  layers = [aws_lambda_layer_version.shared_layer.arn]
}