############################### CREATE LAMBDAS ###############################
# Creates ingestion lambda using ingestion.zip and a pg8000 layer
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
# Creates process lambda using process.zip and the layers: pg8000, forex-Python, AWSSDKPandas
resource "aws_lambda_function" "process_lambda" {
  filename      = "./zipped/process.zip"
  source_code_hash = data.archive_file.zip_process.output_base64sha256
  function_name = "process_lambda"
  role          = aws_iam_role.process_lambda_role.arn
  runtime       = "python3.11"
  handler = "process.handler"
  timeout = 180
  layers = [aws_lambda_layer_version.pg8000_layer.arn,
            aws_lambda_layer_version.forex_layer.arn, 
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
}
# Creates load lambda using load.zip and the layers: pg8000, forex-Python, AWSSDKPandas
resource "aws_lambda_function" "load_lambda" {
  filename      = "./zipped/load.zip"
  source_code_hash = data.archive_file.zip_load.output_base64sha256
  function_name = "load_lambda"
  role          = aws_iam_role.load_lambda_role.arn
  runtime       = "python3.11"
  handler = "load.handler"
  timeout = 180
  layers = [aws_lambda_layer_version.pg8000_layer.arn,
            aws_lambda_layer_version.forex_layer.arn, 
            "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python311:2"]
}
############################### CREATE LAYERS ###############################
# Creates pg8000 layer
resource "aws_lambda_layer_version" "pg8000_layer" {
  filename = "./zipped/pg8000_layer/python.zip"
  layer_name = "pg8000_layer"
  compatible_runtimes = ["python3.11"]
}
# Creates forex-Python layer
resource "aws_lambda_layer_version" "forex_layer" {
  filename = "./zipped/forex-python_layer/python.zip"
  layer_name = "forex-python_layer"
  compatible_runtimes = ["python3.11"]
}
