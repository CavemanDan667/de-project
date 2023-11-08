resource "aws_lambda_function" "ingestion_lambda" {
  filename      = "./zipped/ingestion.zip"
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingestion_lambda_role.arn
  runtime       = "python3.11"
  handler = "ingestion.handler"
  timeout = 30
  layers = [aws_lambda_layer_version.pg8000_layer.arn]
  source_code_hash = data.archive_file.zip_ingestion.output_base64sha256
  environment {
    variables = {
      user = var.USER,
      host = var.HOST,
      database = var.DATABASE,
      password = var.PASSWORD,
      port = var.PORT}
  }
}

resource "aws_lambda_layer_version" "pg8000_layer" {
  filename = "./zipped/pg8000_layer/python.zip"
  layer_name = "pg8000_layer"
  compatible_runtimes = ["python3.11"]
}