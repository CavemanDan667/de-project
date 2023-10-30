resource "aws_iam_policy" "cloudwatch_logs_policy" {
    name = "placeholder-create-write-logs-policy"
    path = "/"
    policy = jsonencode({
        Version = "2012-10-17",
        Statement = [
            {
                Action = ["logs:CreateLogGroup", ]
                Effect = "Allow"
                Resource = "arn:aws:logs:eu-west-2:${local.account_id}:*"
            },
            {
                Action = ["logs:CreateLogStream", "logs:PutLogEvents"]
                Effect = "Allow"
                Resource = "arn:aws:logs:eu-west-2:${local.account_id}:log-group:*"
            }
        ]
    })
}

resource "aws_iam_role" "placeholder_lambda_role" {
    name = "placeholder_lambda_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Sid = ""
        Principal = {
            Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "placeholder_attach" {
  role = aws_iam_role.placeholder_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}