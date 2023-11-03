import csv
import logging
import boto3
from botocore.exceptions import ClientError

def get_client(service_name):
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_csv(now, table_name, data):
    """
    This function takes a timestamp, a table name and
    a dictionary of headers and rows as arguments.
    It then extracts data from the passed in
    dictionary and writes it in a csv file to a
    with a name created by the parameters.


    Args:
        now: (string) a timestamp of when the data is created
        table_name(string): the name of the table a data
        data (dictionary): the response data from fetch_data function

    Returns:
        none

    Raises:
        none

    """

    try:

        csvfile = open("/tmp/data.csv", "w", newline="")
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(data["Headers"])
        csv_writer.writerows(data["Rows"])
        csvfile.close()
        upload_object(now, table_name, csvfile.name)
    except KeyError as e:
        logger.error(f"KeyError: key {e} not found")
        raise e
    except csv.Error as e:
        logger.error(f"csv.Error: invalid data type: {e}")
        raise e

    


def upload_object(now, table_name, file_name):
    try:

        s3 = get_client("s3")
        s3.upload_file(
            file_name,
            "de-project-ingestion-bucket",
            f"{table_name}/{now}.csv"
        )
    except FileNotFoundError as e:
        logger.error(f"File not found")
        raise e


    csvfile = open("/tmp/data.csv", "w", newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(data["Headers"])
    csv_writer.writerows(data["Rows"])
    csvfile.close()
    upload_object(now, table_name, csvfile.name)


def upload_object(now, table_name, file_name):
    '''
    This function takes the timestamp, the table name and
    the dictionary of headers and rows as arguments, passed
    in via write_data_to_csv function.
    It then uploads the file created to a pre-made bucket,
    with a path created by the table name and timestamp.

    Args:
        now: (string) a timestamp of when the data is created
        table_name(string): the name of the table a data
        file_name: the file_name to be uploaded

    Returns:
        none

    Raises:
        none

    '''
    s3 = get_client("s3")
    s3.upload_file(
        file_name,
        "de-project-ingestion-bucket",
        f"{table_name}/{now}.csv"
    )

