data "aws_caller_identity" "current_user" {
}

locals {
  account_id = data.aws_caller_identity.current_user.account_id
}

output "account_id" {
  value = local.account_id
}

data "archive_file" "zip_load" {
  type = "zip"
  source_dir = "../src/load_dummy"
  output_path = "./zipped/load.zip"
}