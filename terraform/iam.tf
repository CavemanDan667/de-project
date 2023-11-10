resource "aws_iam_policy" "cloudwatch_logs_policy" {
    name = "ingestion-create-write-logs-policy"
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

resource "aws_iam_policy" "s3_read_write_policy" {
    name = "ingestion-s3-read-write-policy"
    path = "/"
    policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": ["s3:ListBucket"],
            "Resource": ["arn:aws:s3:::de-project-ingestion-bucket"]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": ["arn:aws:s3:::de-project-ingestion-bucket/*"]
        }
    ]
  })
}


resource "aws_iam_role" "ingestion_lambda_role" {
    name = "ingestion_lambda_role"
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

resource "aws_iam_role_policy_attachment" "cloudwatch_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_read_write_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "secrets_read_write_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

resource "aws_iam_role_policy_attachment" "admin_access_ingest_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}


resource "aws_iam_role" "process_lambda_role" {
    name = "process_lambda_role"
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

resource "aws_iam_role_policy_attachment" "secrets_read_write_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_read_write_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "S3_Full_access_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "admin_access_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_iam_role" "load_lambda_role" {
    name = "load_lambda_role"
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

resource "aws_iam_role_policy_attachment" "cloudwatch_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}

resource "aws_iam_role_policy_attachment" "s3_read_write_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "S3_Full_access_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "secrets_read_write_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}

resource "aws_iam_role_policy_attachment" "admin_access_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}