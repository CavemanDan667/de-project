resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name = "ErrorAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  threshold = 0
  period = 600
  metric_name = "ErrorCount"
  statistic = "Sum"
  alarm_actions = [aws_sns_topic.ingestion_alerts.arn]
  namespace = "CustomLambdaMetrics"
}


resource "aws_cloudwatch_metric_alarm" "created_alarm" {
  alarm_name = "CreatedAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 6
  threshold = 0
  period = 600
  metric_name = "CreatedCount"
  statistic = "Sum"
  alarm_actions = [aws_sns_topic.ingestion_alerts.arn]
  namespace = "CustomLambdaMetrics"
}


resource "aws_sns_topic" "ingestion_alerts" {
  name = "ingestion_alerts"
}


# Provides a resource for subscribing to SNS topics. 
# Requires that an SNS topic exist for the subscription to attach to. 
resource "aws_sns_topic_subscription" "sns_subscription" {
  for_each	= toset([
    var.EMAIL_1, 
    var.EMAIL_2, 
    var.EMAIL_3, 
    var.EMAIL_4, 
    var.EMAIL_5])
  topic_arn = aws_sns_topic.ingestion_alerts.arn
  protocol  = "email"
  endpoint  = each.value
}





# # Use this data source to get the ARN of a topic in AWS Simple Notification Service (SNS). 
# # By using this data source, you can reference SNS topics without having to hard code the ARNs as input.
# data "aws_sns_topic" "sns_topic_data" {
#   name = "user-updates-topic"
# }




# CloudWatch Log Metric Filter resource - 'Error' Filter
resource "aws_cloudwatch_log_metric_filter" "error_filter" {
  name           = "ErrorFilter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/ingestion_lambda"

  metric_transformation {
    name      = "ErrorCount"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}


# CloudWatch Log Metric Filter resource - 'Created' Filter
resource "aws_cloudwatch_log_metric_filter" "created_filter" {
  name           = "CreatedFilter"
  pattern        = "CREATED"
  log_group_name = "/aws/lambda/ingestion_lambda"

  metric_transformation {
    name      = "CreatedCount"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}


