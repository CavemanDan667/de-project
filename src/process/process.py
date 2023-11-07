import os
import logging
from process_utils.write_to_parquet import write_data_to_parquet


user = os.environ["user"]
host = os.environ["host"]
database = os.environ["database"]
password = os.environ["password"]
port = os.environ["port"]


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    file_name = event['Records'][0]['s3']['object']['key']
    now = file_name.split('/')[1][:-4]
    table = file_name.split('/')[0]
    data_frame = 'some function'
    write_data_to_parquet(now, table, data_frame)
