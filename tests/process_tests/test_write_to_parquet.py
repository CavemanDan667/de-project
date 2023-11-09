from src.process.process_utils.write_to_parquet import (
    write_data_to_parquet
)
import os
import boto3
import pytest
from moto import mock_s3
import pandas as pd


@pytest.fixture
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3")


def test_writes_data_to_parquet_and_uploads_objects(s3, caplog):
    s3.create_bucket(
        Bucket="de-project-processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    data = {
        'column_1': [1, 2, 3],
        'column_2': ['one', 'two', 'three'],
        'column_3': [10, 20, 30],
        'last_updated': ['2020-01-01', '2020-01-01', '2023-11-01']
    }
    test_data_frame = pd.DataFrame(data=data)

    write_data_to_parquet("0002", "currency", data_frame=test_data_frame)
    list_of_objects = s3.list_objects_v2(
        Bucket="de-project-processed-bucket"
    )
    assert list_of_objects['Contents'][0]['Key'] == 'currency/0002.parquet'
