resource "aws_lambda_function" "placeholder_lambda" {
  filename      = "../zipped/demo.zip"
  function_name = "placeholder_lambda"
  role          = aws_iam_role.placeholder_lambda_role.arn
  runtime       = "python3.11"
  handler = "demo.handler"
}