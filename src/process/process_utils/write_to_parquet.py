import logging
import boto3


def get_client(service_name):
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_parquet(now, table_name, data_frame):
    data_frame.to_parquet("/tmp/data.parquet")
    upload_object(now, table_name, 'data.parquet')


def upload_object(now, table_name, file_name):
    s3 = get_client("s3")
    s3.upload_file(
        file_name, "de-project-processed-bucket", f"{table_name}/{now}.parquet"
    )
