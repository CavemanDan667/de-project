############################### USER DATA ###############################
# Creates a variable with current users' account id
data "aws_caller_identity" "current_user" {
}

locals {
  account_id = data.aws_caller_identity.current_user.account_id
}

output "account_id" {
  value = local.account_id
}

############################### ZIP FUNCTIONS ###############################
# Zips up the ingestion folder on terraform apply for use in the lambda
data "archive_file" "zip_ingestion" {
  type = "zip"
  source_dir = "../src/ingestion"
  output_path = "./zipped/ingestion.zip"
}
# Zips up the process folder on terraform apply for use in the lambda
data "archive_file" "zip_process" {
  type = "zip"
  source_dir = "../src/process"
  output_path = "./zipped/process.zip"
}
# Zips up the loading folder on terraform apply for use in the lambda
data "archive_file" "zip_load" {
  type = "zip"
  source_dir = "../src/loading"
  output_path = "./zipped/load.zip"
}