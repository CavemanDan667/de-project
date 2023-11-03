import os
from moto import mock_s3
import boto3
import pytest
from src.ingestion.ingestion_utils.write_data import write_data_to_csv, upload_object # noqa


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


    def test_upload_object_returns_correct_object_name_and_logs_creation_message(self, s3, caplog):  # noqa
        s3.create_bucket(
            Bucket="de-project-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        upload_object("0001", "currency", "test.txt")
        list_of_objects = s3.list_objects_v2(
                Bucket="de-project-ingestion-bucket")
        assert list_of_objects["Contents"][0]["Key"] == "currency/0001.csv" # noqa


    def test_write_data_converts_dictionary_into_csv_and_uploads_to_s3_bucket(self, s3, caplog): # noqa
        s3.create_bucket(
            Bucket="de-project-ingestion-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        data = {
            'Headers': ['column_1', 'column_2', 'column_3', 'last_updated'],
            'Rows': [
                [1, 'one', 10, (2020, 1, 1, 0, 0)],
                [2, 'two', 20, (2020, 1, 1, 0, 0)],
                [3, 'three', 30, (2023, 11, 1, 0, 0)]
            ]
        }

        write_data_to_csv("0001", "currency", data)
        list_of_objects = s3.list_objects_v2(
            Bucket="de-project-ingestion-bucket")
        response_body = s3.get_object(Bucket="de-project-ingestion-bucket",
                                      Key="currency/0001.csv")["Body"].read().decode("utf-8") # noqa

        assert list_of_objects["Contents"][0]["Key"] == "currency/0001.csv"
        assert 'column_1,column_2,column_3,last_updated\r\n' in response_body
        assert '1,one,10,"(2020, 1, 1, 0, 0)"\r\n2,' in response_body
        assert '2,two,20,"(2020, 1, 1, 0, 0)"\r\n3,' in response_body
        assert '3,three,30,"(2023, 11, 1, 0, 0)"\r\n' in response_body
