resource "aws_lambda_layer_version" "shared_layer" {
    filename = "ingestion_utils.zip"
    description = "Ingestion Shared Lambda Layer"
    layer_name = "project-shared-layer"
}

