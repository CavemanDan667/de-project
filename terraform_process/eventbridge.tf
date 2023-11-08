# resource "aws_lambda_permission" "allow_cloudwatch_to_call_ingestion_lambda" {
#     statement_id = "AllowExecutionFromCloudWatch"
#     action = "lambda:InvokeFunction"
#     function_name = aws_lambda_function.ingestion_lambda.function_name
#     principal = "events.amazonaws.com"
#     source_arn = aws_cloudwatch_event_rule.every_five_minutes.arn
# }

