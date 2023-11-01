from src.ingestion.ingestion_utils.write_data import write_data_to_csv, upload_object
import os
from moto import mock_s3
import boto3
import pytest


class TestUploadObject:
    @pytest.fixture
    def aws_credentials(self):
        """Mocked AWS Credentials for moto."""
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_SECURITY_TOKEN"] = "testing"
        os.environ["AWS_SESSION_TOKEN"] = "testing"
        os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

    @pytest.fixture
    def s3(self, aws_credentials):
        with mock_s3():
            yield boto3.client("s3")

    def test_should_call_s3_upload_file_with_the_correct_parameters(): #noqa
        pass