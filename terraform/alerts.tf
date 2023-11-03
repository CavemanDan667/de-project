resource "aws_sns_topic" "alarm" {
  name = "user-updates-topic"
  delivery_policy = <<EOF
{
  "http": {
    "defaultHealthyRetryPolicy": {
      "minDelayTarget": 20,
      "maxDelayTarget": 20,
      "numRetries": 3,
      "numMaxDelayRetries": 0,
      "numNoDelayRetries": 0,
      "numMinDelayRetries": 0,
      "backoffFunction": "linear"
    },
    "disableSubscriptionOverrides": false,
    "defaultThrottlePolicy": {
      "maxReceivesPerSecond": 1
    }
  }
}
EOF
}


# Use this data source to get the ARN of a topic in AWS Simple Notification Service (SNS). 
# By using this data source, you can reference SNS topics without having to hard code the ARNs as input.
data "aws_sns_topic" "sns_topic_data" {
  name = "user-updates-topic"
}


# Provides a resource for subscribing to SNS topics. 
# Requires that an SNS topic exist for the subscription to attach to. 
resource "aws_sns_topic_subscription" "sns_subscription" {
  for_each	= toset([email_address1, email_address2, etc])
  topic_arn = data.aws_sns_topic.sns_topic_data.arn
  protocol  = "email"
  endpoint  = each.value
}


# CloudWatch Log Metric Filter resource
resource "aws_cloudwatch_log_metric_filter" "metric_filter" {
  name           = "MyAppAccessCount"
  pattern        = ""
  log_group_name = "arn:aws:logs:eu-west-2:${local.account_id}:*"

  metric_transformation {
    name      = "ErrorCount"
    namespace = "???YourNamespace"
    value     = "1"
  }
}
