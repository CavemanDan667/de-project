############################### INVOKE LAMBDA ###############################
# Creates an event rule to trigger every 5 minutes
resource "aws_cloudwatch_event_rule" "every_five_minutes" {
    name = "every-five-minutes"
    description = "Fires every five minutes"
    schedule_expression = "rate(5 minutes)"
}
# Connects the event rule with the ingestion lambda
resource "aws_cloudwatch_event_target" "check_foo_every_five_minutes" {
    rule = aws_cloudwatch_event_rule.every_five_minutes.name
    target_id = "ingestion_lambda"
    arn = aws_lambda_function.ingestion_lambda.arn
}
# Gives permission to the ingestion lambda to be invoked from eventbridge
resource "aws_lambda_permission" "allow_cloudwatch_to_call_ingestion_lambda" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.ingestion_lambda.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_five_minutes.arn
}

