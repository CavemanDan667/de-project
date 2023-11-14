############################### SECRETS MANAGER ###############################
# Creates a secret for totesys database credentials with forced deletion
resource "aws_secretsmanager_secret" "totesys_db_creds" {
  name = "totesys_database_creds"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}
# Stores totesys database credentials in the secret
resource "aws_secretsmanager_secret_version" "totesys_db_creds" {
  secret_id = aws_secretsmanager_secret.totesys_db_creds.id
  secret_string = jsonencode(var.totesys_db_creds)
}
# Creates a secret for data warehouse credentials with forced deletion
resource "aws_secretsmanager_secret" "data_warehouse_creds" {
  name = "data_warehouse_creds"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}
# Stores data warehouse credentials in the secret
resource "aws_secretsmanager_secret_version" "data_warehouse_creds" {
  secret_id = aws_secretsmanager_secret.data_warehouse_creds.id
  secret_string = jsonencode(var.data_warehouse_creds)
}
# Creates a secret for test totesys database credentials with forced deletion
resource "aws_secretsmanager_secret" "test_totesys_db_creds" {
  name = "test_totesys_db_creds"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}
# Stores test totesys database credentials in the secret
resource "aws_secretsmanager_secret_version" "test_totesys_db_creds" {
  secret_id = aws_secretsmanager_secret.test_totesys_db_creds.id
  secret_string = jsonencode(var.test_totesys_db_creds)
}
# Creates a secret for test data warehouse credentials with forced deletion
resource "aws_secretsmanager_secret" "test_dw_creds" {
  name = "test_dw_creds"
  force_overwrite_replica_secret = true
  recovery_window_in_days = 0
}
# Stores test data warehouse credentials in the secret
resource "aws_secretsmanager_secret_version" "test_dw_creds" {
  secret_id = aws_secretsmanager_secret.test_dw_creds.id
  secret_string = jsonencode(var.test_dw_creds)
}
