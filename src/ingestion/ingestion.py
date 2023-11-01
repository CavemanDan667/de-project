import logging
import csv
import datetime
import time
from ingestion_utils.write_data import write_data_to_csv


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    This handler invokes write data to csv to create a csv in an s3 bucket based on the dictionary that is passed
    Args - Event, Context - currently unused
    """
    logger.info("Creating a CSV file")
    
    now = int(time.time())
    
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

