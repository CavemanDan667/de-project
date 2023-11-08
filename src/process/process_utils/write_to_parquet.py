import logging
import boto3


def get_client(service_name):
    """Returns a boto3 client connection.

    Args:
        service_name (string): The name of the service
        to which the function connects.

    Returns:
        A boto3 client connection.
    """
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_parquet(now, table_name, data_frame):
    """Converts data to parquet and uploads to S3 bucket.

    Args:
        now (string): unix timestamp
        table_name (string): Name of the database table where
            the data originated.
        data_frame (string): The data which is to be
            converted into parquet form.
    """
    data_frame.to_parquet("/tmp/data.parquet")
    upload_object(now, table_name, 'data.parquet')


def upload_object(now, table_name, file_name):
    """Uploads a file into the bucket 'de-project-processed-bucket'.

    Args:
        now (string): unix timestamp
        table_name (string): Name of the database table where
            the data originated.
        file_name (string): The file to be uploaded.
    """
    s3 = get_client("s3")
    s3.upload_file(
        file_name, "de-project-processed-bucket", f"{table_name}/{now}.parquet"
    )
