import logging
import csv
import datetime
import time
from ingestion_utils.get_client import get_client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    This handler invokes write data to csv to create a csv in an s3 bucket based on the dictionary that is passed
    Args - Event, Context - currently unused
    """
    logger.info("Creating a CSV file")
    # Now lists the number of seconds since the epoch
    now = int(time.time())
    # Table name and data should be obtained through fetch data
    table_name = "currency"
    data = {
        "Headers": ["currency_id", "currency_code", "created_at", "last_updated"],
        "Rows": [
            [
                1,
                "GBP",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
            [
                2,
                "USD",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
            [
                3,
                "EUR",
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
            ],
        ],
    }

    write_data_to_csv(now, table_name, data)


# These below need to extracted out into their own files
def upload_object(now, table_name, file_name):
    s3 = get_client("s3")
    s3.upload_file(file_name, "de-project-ingestion-bucket", f"{table_name}/{now}.csv")
    logger.info(f"{table_name}/{now}.csv has been created")


def write_data_to_csv(now, table_name, data):
    csvfile = open("/tmp/data.csv", "w", newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(data["Headers"])
    csv_writer.writerows(data["Rows"])
    csvfile.close()
    upload_object(now, table_name, csvfile.name)

