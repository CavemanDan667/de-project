import logging
import boto3
import awswrangler as wr


def get_client(service_name):
    """Returns a boto3 client connection.

    Args:
        service_name (str):
        The name of the service to which the function connects.

    Returns:
        botocore.client.BaseClient: A boto3 client connection.
    """
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_parquet(now, table_name, data_frame):
    """Writes a pandas DataFrame to Parquet format on s3 bucket.

    Args:
        now (str): A timestamp or identifier for the current operation.
        table_name (str): The name of the table.
        data_frame (pd.DataFrame):
        The pandas DataFrame to be written to Parquet.

    Returns:
        None
    """
    try:
        p = f's3://de-project-transformed-bucket/{table_name}/{now}.parquet'
        wr.s3.to_parquet(
            df=data_frame,
            path=p
        )
        logger.info(f"[CREATED]: {p} was created")
    except Exception as e:
        logger.error(f"Error writing data to Parquet: {e}")
