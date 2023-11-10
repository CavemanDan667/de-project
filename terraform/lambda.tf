resource "aws_lambda_function" "ingestion_lambda" {
  filename      = "./zipped/ingestion.zip"
  function_name = "ingestion_lambda"
  role          = aws_iam_role.ingestion_lambda_role.arn
  runtime       = "python3.11"
  handler = "ingestion.handler"
  timeout = 30
  layers = [aws_lambda_layer_version.pg8000_layer.arn]
  source_code_hash = data.archive_file.zip_ingestion.output_base64sha256
}


resource "aws_lambda_function" "process_lambda" {
  filename      = "./zipped/process.zip"
  source_code_hash = data.archive_file.zip_process.output_base64sha256
  function_name = "process_lambda"
  role          = aws_iam_role.process_lambda_role.arn
  runtime       = "python3.11"
  handler = "process_lambda_dummy.handler"
  timeout = 180
  layers = [aws_lambda_layer_version.pg8000_layer.arn,
            aws_lambda_layer_version.forex_layer.arn, 
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
}


resource "aws_lambda_layer_version" "pg8000_layer" {
  filename = "./zipped/pg8000_layer/python.zip"
  layer_name = "pg8000_layer"
  compatible_runtimes = ["python3.11"]
}

resource "aws_lambda_layer_version" "forex_layer" {
  filename = "./zipped/forex-python_layer/python.zip"
  layer_name = "forex-python_layer"
  compatible_runtimes = ["python3.11"]
}
