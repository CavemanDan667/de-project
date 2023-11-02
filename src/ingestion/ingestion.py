import logging
import datetime
import time
from pg8000.native import Connection
from ingestion_utils.write_data import write_data_to_csv
from ingestion_utils.fetch_data import fetch_data
from ingestion_utils.get_tables import get_table_names
from ingestion_utils.extract_newest_time import extract_newest_time
from ingestion_utils.list_s3_contents import list_contents
import os
user = os.environ['user']
host = os.environ['host']
database = os.environ['database']
password = os.environ['password']
port = os.environ['port']

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    This handler invokes write data to csv to create a .csv
    in an s3 bucket based on the dictionary that is passed

    Args - Event, Context - currently unused
    """

    unix_now = int(time.time())
    conn = Connection(user=user, host=host, database=database, port=port, password=password)

    bucket_filenames = list_contents("de-project-ingestion-bucket")
    newest_time = extract_newest_time(bucket_filenames)

    table_names = get_table_names(conn)

    dt_now = datetime.datetime.fromtimestamp(unix_now)
    dt_newest = datetime.datetime.fromtimestamp(newest_time)

    for table_name in table_names:
        data = fetch_data(conn, table_name, dt_newest, dt_now)
        if len(data['Rows']) != 0:
            write_data_to_csv(unix_now, table_name, data)
            logger.info(f"[CREATED]: {table_name}/{unix_now}.csv has been created")
        else:
            logger.info(f'{table_name} had no new data')
