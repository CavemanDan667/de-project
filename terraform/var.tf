variable "USER" {
  type = string
}

variable "HOST" {
  type = string
}

variable "DATABASE" {
  type = string
}

variable "PASSWORD" {
  type = string
}

variable "PORT" {
  type = string
}

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