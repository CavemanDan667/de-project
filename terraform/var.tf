############################### VARIABLES ###############################
# Email inputs for alerts
variable "EMAIL_1" {
  type = string
}

variable "EMAIL_2" {
  type = string
}

variable "EMAIL_3" {
  type = string
}

variable "EMAIL_4" {
  type = string
}

variable "EMAIL_5" {
  type = string
}
# Totesys database credentials
variable "totesys_db_creds" {
  type = object({
    TOTESYS_USER = string
    TOTESYS_HOST = string
    TOTESYS_DATABASE = string
    TOTESYS_PASSWORD = string
    TOTESYS_PORT = string
  })
  sensitive = true
  nullable = false
}
# Data warehouse credentials
variable "data_warehouse_creds" {
  type = object({
    DW_USER = string
    DW_HOST = string
    DW_DATABASE = string
    DW_PASSWORD = string
    DW_PORT = string
  })
  sensitive = true
  nullable = false
}
# Test totesys database credentials
variable "test_totesys_db_creds" {
  type = object({
    TEST_USER = string
    TEST_HOST = string
    TEST_DATABASE = string
    TEST_PASSWORD = string
    TEST_PORT = string
  })
  sensitive = true
  nullable = false
}
# Test data warehouse credentials
variable "test_dw_creds" {
  type = object({
    TESTDW_USER = string
    TESTDW_HOST = string
    TESTDW_DATABASE = string
    TESTDW_PASSWORD = string
    TESTDW_PORT = string
  })
  sensitive = true
  nullable = false
}