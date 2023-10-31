from src.utils.list_s3_contents import list_contents
import os
from moto import mock_s3
import boto3
import pytest


class TestListS3:
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

    @mock_s3
    def test_list_contents_returns_empty_list_if_bucket_is_empty(self, s3):  # noqa
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        response = list_contents("test-bucket")
        assert response == []

    def test_list_contents_returns_a_list_with_one_item_if_only_one_present(
        self, s3
    ):  # noqa
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.upload_file("test.txt", "test-bucket", "tablename/546530.csv")
        response = list_contents("test-bucket")
        assert response == ["tablename/546530.csv"]

    def test_list_contents_returns_a_list_with_every_item(self, s3):  # noqa
        s3.create_bucket(
            Bucket="test-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3.upload_file("test.txt", "test-bucket", "tablename/1.csv")
        s3.upload_file("test.txt", "test-bucket", "tablename/1000.csv")
        s3.upload_file("test.txt", "test-bucket", "tablename/7.csv")
        s3.upload_file("test.txt", "test-bucket", "tablename/3.csv")
        s3.upload_file("test.txt", "test-bucket", "tablename/8.csv")
        response = list_contents("test-bucket")
        print(response)
        assert response == [
            "tablename/1.csv",
            "tablename/1000.csv",
            "tablename/3.csv",
            "tablename/7.csv",
            "tablename/8.csv",
        ]
