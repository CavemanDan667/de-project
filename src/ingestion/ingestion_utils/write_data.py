import csv
import logging
import boto3


def get_client(service_name):
    client = boto3.client(service_name)
    return client


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_data_to_csv(now, table_name, data):
    """
    This function takes a dictionary as an arguement, which
    is passed from the response of fetch_data; a dictionary
    of headers and rows.

    The function then extracts data from the passed in dictionary
    and writes it in a csv file at the provided filepath.
    """
    csvfile = open("/tmp/data.csv", "w", newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(data["Headers"])
    csv_writer.writerows(data["Rows"])
    csvfile.close()
    upload_object(now, table_name, csvfile.name)


def upload_object(now, table_name, file_name):
    s3 = get_client("s3")
    s3.upload_file(
        file_name,
        "de-project-ingestion-bucket",
        f"{table_name}/{now}.csv"
    )
