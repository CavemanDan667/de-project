import logging
import datetime
import time
from pg8000.native import Connection
from ingestion_utils.write_data import write_data_to_csv
from ingestion_utils.fetch_data import fetch_data
from ingestion_utils.get_tables import get_table_names
from ingestion_utils.extract_newest_time import extract_newest_time
from ingestion_utils.list_s3_contents import list_contents
from ingestion_utils.get_credentials import get_credentials


config = get_credentials('totesys_database_creds')

user = config["TOTESYS_USER"]
host = config["TOTESYS_HOST"]
database = config["TOTESYS_DATABASE"]
password = config["TOTESYS_PASSWORD"]
port = config["TOTESYS_PORT"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """Manages the invocation of several functions to retrieve any new data
    from a database and store it in an s3 bucket as a csv file.

    Calls:
        list_contents: to retrieve a list of every item in an s3 bucket.
        get_table_names: to retrieve a list of all table names from a database.
        extract_newest_time: to find the unix timestamp for the most recently
            created file in a specific table.
        fetch_data: to retrieve a dictionary with the contents of the
            database that have been updated between the current time
            and the newest time.
        write_data_to_csv: to convert the passed in dataframe to a csv
            file that is then stored in the ingestion s3 bucket.

    Args:
        event (dict), context (dict):
            Needed to correctly run the function as a lambda.

    Raises:
        Exception: Errors that have been raised by utility functions.
    """
    try:
        unix_now = int(time.time())

        conn = Connection(
            user=user,
            host=host,
            database=database,
            port=port,
            password=password
        )

        bucket_filenames = list_contents("de-project-ingestion-bucket")
        table_names = get_table_names(conn)
        dt_now = datetime.datetime.fromtimestamp(unix_now)

        for table_name in table_names:
            newest_time = extract_newest_time(bucket_filenames, table_name)
            dt_newest = datetime.datetime.fromtimestamp(newest_time)
            data = fetch_data(conn, table_name, dt_newest, dt_now)

            if len(data["Rows"]) != 0:
                write_data_to_csv(unix_now, table_name, data)
                logger.info(
                    f"[CREATED]: {table_name}/{unix_now}.csv has been created"
                )
            else:
                logger.info(f"{table_name} had no new data")

    except Exception as e:
        logger.error(f"handler has raised {e}")
        raise e
