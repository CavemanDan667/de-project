data "aws_caller_identity" "current_user" {
}
locals {
  account_id = data.aws_caller_identity.current_user.account_id
}
output "account_id" {
  value = local.account_id
}

data "archive_file" "zipit" {
  type = "zip"
  source_file = "../src/demo.py"
  output_path = "./zipped/demo.zip"
}