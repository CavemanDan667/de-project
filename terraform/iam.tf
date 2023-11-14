############################### CREATE IAM POLICIES ###############################
# Creates a policy that allows the creation of a log group and write to it
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
# Creates a policy that allows interaction with an ingestion S3 bucket
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

############################### CREATE IAM ROLES ###############################
# Creates an ingestion lambda IAM role
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
# Creates an process lambda IAM role
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
# Creates an load lambda IAM role
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

############################### IAM POLICY ATTACHMENTS ###############################
############################### INGESTION ATTACHMENTS ###############################
# Attaches the cloudwatch policy to ingestion
resource "aws_iam_role_policy_attachment" "cloudwatch_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}
# Attaches the read/write policy to ingestion
resource "aws_iam_role_policy_attachment" "s3_read_write_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}
# Attaches secretsmanager read/write policy to ingestion
resource "aws_iam_role_policy_attachment" "secrets_read_write_ingestion_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}
# Attaches admin access policy to ingestion
resource "aws_iam_role_policy_attachment" "admin_access_ingest_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
############################### PROCESS ATTACHMENTS ###############################
# Attaches secretsmanager read/write policy to process
resource "aws_iam_role_policy_attachment" "secrets_read_write_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}
# Attaches the cloudwatch policy to process
resource "aws_iam_role_policy_attachment" "cloudwatch_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}
# Attaches the read/write policy to process
resource "aws_iam_role_policy_attachment" "s3_read_write_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}
# Attaches the full access policy to process
resource "aws_iam_role_policy_attachment" "S3_Full_access_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
# Attaches admin access policy to process
resource "aws_iam_role_policy_attachment" "admin_access_process_attach" {
  role = aws_iam_role.process_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}
############################### LOADING ATTACHMENTS ###############################
# Attaches cloudwatch policy to load
resource "aws_iam_role_policy_attachment" "cloudwatch_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.cloudwatch_logs_policy.arn
}
# Attaches read/write policy to load
resource "aws_iam_role_policy_attachment" "s3_read_write_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = aws_iam_policy.s3_read_write_policy.arn
}
# Attaches full access policy to load
resource "aws_iam_role_policy_attachment" "S3_Full_access_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}
# Attaches full access policy to load
resource "aws_iam_role_policy_attachment" "secrets_read_write_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/SecretsManagerReadWrite"
}
# Attaches admin access policy to load
resource "aws_iam_role_policy_attachment" "admin_access_load_attach" {
  role = aws_iam_role.load_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}