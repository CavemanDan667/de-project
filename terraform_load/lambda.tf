resource "aws_lambda_function" "load_lambda" {
  filename      = "./zipped/load.zip"
  source_code_hash = data.archive_file.zip_load.output_base64sha256
  function_name = "load_lambda"
  role          = aws_iam_role.load_lambda_role.arn
  runtime       = "python3.11"
  handler = "load_lambda_dummy.handler"
  timeout = 180
  environment {
    variables = {
      user = var.USER,
      host = var.HOST,
      database = var.DATABASE,
      password = var.PASSWORD,
      port = var.PORT}
  }
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
