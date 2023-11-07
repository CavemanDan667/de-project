# Creates an SNS topic thread
resource "aws_sns_topic" "ingestion_alerts" {
  name = "ingestion_alerts"
}

# Sets an alarm monitoring the Error metric
resource "aws_cloudwatch_metric_alarm" "error_alarm" {
  alarm_name = "ErrorAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  threshold = 0
  period = 600
  metric_name = "ErrorCount"
  statistic = "SampleCount"
  alarm_actions = [aws_sns_topic.ingestion_alerts.arn]
  namespace = "CustomLambdaMetrics"
  treat_missing_data = "notBreaching"
}

# Sets an alarm monitoring the Created metric
resource "aws_cloudwatch_metric_alarm" "created_alarm" {
  alarm_name = "CreatedAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 6
  datapoints_to_alarm = 5
  threshold = 0
  period = 600
  metric_name = "CreatedCount"
  statistic = "SampleCount"
  alarm_actions = [aws_sns_topic.ingestion_alerts.arn]
  namespace = "CustomLambdaMetrics"
  treat_missing_data = "notBreaching"
}

# Provides a resource for subscribing to the SNS topic.
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


# Creates a CloudWatch Log Metric Filter resource - 'Error' Filter
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


# Creates a CloudWatch Log Metric Filter resource - 'Created' Filter
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
