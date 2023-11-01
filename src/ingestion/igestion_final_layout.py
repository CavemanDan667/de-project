import logging
import datetime
import time
from pg8000.native import Connection
from ingestion_utils.write_data import write_data_to_csv
from ingestion_utils.fetch_data import fetch_data
from ingestion_utils.get_tables import get_table_names
from ingestion_utils.connection import get_connection
from ingestion_utils.extract_newest_time import extract_newest_time
from ingestion_utils.list_s3_contents import list_contents


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    This handler invokes write data to csv to create a csv in an s3 bucket based on the dictionary that is passed
    Args - Event, Context - currently unused
    """
    logger.info("Creating a CSV file")
    
    unix_now = int(time.time())
    conn = Connection()

    bucket_filenames = list_contents("de-project-ingestion-bucket")
    newest_time = extract_newest_time(bucket_filenames)

    table_names = get_table_names(conn)

    
    dt_now = datetime.datetime.fromtimestamp(unix_now)
    dt_newest = datetime.datetime.fromtimestamp(newest_time)


    for table_name in table_names:
        data = fetch_data(conn, table_name, dt_newest, dt_now)
        write_data_to_csv(unix_now, table_name, data)

