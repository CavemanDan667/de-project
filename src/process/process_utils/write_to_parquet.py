import logging
import boto3
import awswrangler as wr
import pandas as pd
def get_client(service_name):
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_parquet(now, table_name, data_frame):
    p=f's3://de-project-processed-bucket/{table_name}/{now}.parquet'
    wr.s3.to_parquet(
        df=data_frame,
        path=p
    )


