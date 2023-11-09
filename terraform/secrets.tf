resource "aws_secretsmanager_secret" "totesys_db_creds" {
  name = "totesys_database_credentials"
}

resource "aws_secretsmanager_secret_version" "totesys_db_creds" {
  secret_id = aws_secretsmanager_secret.totesys_db_creds.id
  secret_string = jsonencode(var.totesys_db_creds)
}

resource "aws_secretsmanager_secret" "data_warehouse_creds" {
  name = "data_warehouse_credentials"
}

resource "aws_secretsmanager_secret_version" "data_warehouse_creds" {
  secret_id = aws_secretsmanager_secret.data_warehouse_creds.id
  secret_string = jsonencode(var.data_warehouse_creds)
}