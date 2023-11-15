# Creates an SNS topic thread
resource "aws_sns_topic" "alerts" {
  name = "alerts"
}

# Provides a resource for subscribing to the SNS topic.
resource "aws_sns_topic_subscription" "sns_subscription" {
  for_each	= toset([
    var.EMAIL_1, 
    var.EMAIL_2, 
    var.EMAIL_3, 
    var.EMAIL_4, 
    var.EMAIL_5])
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = each.value
}


############################### INGESTION LAMBDA ###############################
# Creates a CloudWatch Log Metric Filter resource - 'Error' Filter
resource "aws_cloudwatch_log_metric_filter" "ingestion_error_filter" {
  name           = "IngestionErrorFilter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/ingestion_lambda"

  metric_transformation {
    name      = "IngestionErrorCount"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

# Sets an alarm monitoring the Error metric
resource "aws_cloudwatch_metric_alarm" "ingestion_error_alarm" {
  alarm_name = "IngestionErrorAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  threshold = 0
  period = 600
  metric_name = "IngestionErrorCount"
  statistic = "SampleCount"
  alarm_actions = [aws_sns_topic.alerts.arn]
  namespace = "CustomLambdaMetrics"
  treat_missing_data = "notBreaching"
}

############################### TRANSFORMATION LAMBDA ###############################
# Creates a CloudWatch Log Metric Filter resource - 'Error' Filter
resource "aws_cloudwatch_log_metric_filter" "transform_error_filter" {
  name           = "TransformErrorFilter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/transform_lambda"

  metric_transformation {
    name      = "TransformErrorCount"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

# Sets an alarm monitoring the Error metric
resource "aws_cloudwatch_metric_alarm" "transform_error_alarm" {
  alarm_name = "TransformErrorAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  threshold = 0
  period = 600
  metric_name = "TransformErrorCount"
  statistic = "SampleCount"
  alarm_actions = [aws_sns_topic.alerts.arn]
  namespace = "CustomLambdaMetrics"
  treat_missing_data = "notBreaching"
}



############################### LOAD LAMBDA ###############################
# Creates a CloudWatch Log Metric Filter resource - 'Error' Filter
resource "aws_cloudwatch_log_metric_filter" "load_error_filter" {
  name           = "LoadErrorFilter"
  pattern        = "ERROR"
  log_group_name = "/aws/lambda/load_lambda"

  metric_transformation {
    name      = "LoadErrorCount"
    namespace = "CustomLambdaMetrics"
    value     = "1"
  }
}

# Sets an alarm monitoring the Error metric
resource "aws_cloudwatch_metric_alarm" "load_error_alarm" {
  alarm_name = "LoadErrorAlarm"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods = 1
  threshold = 0
  period = 600
  metric_name = "LoadErrorCount"
  statistic = "SampleCount"
  alarm_actions = [aws_sns_topic.alerts.arn]
  namespace = "CustomLambdaMetrics"
  treat_missing_data = "notBreaching"
}
