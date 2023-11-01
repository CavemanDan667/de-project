import logging
import csv
import datetime
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Creating a file locally")
    write_data_to_csv({'Headers': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'Rows': [[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], [3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]]})


def upload_object(file_name):
    s3 = get_client("s3")
    s3.upload_file(file_name, "de-project-ingestion-bucket", "test_file.csv")
    logger.info("File created")


def write_data_to_csv(dictionary):
   
    csvfile = open('/tmp/test.csv', 'w', newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(dictionary['Headers'])
    csv_writer.writerows(dictionary['Rows'])
    csvfile.close()
    upload_object(csvfile.name)

def get_client(service_name):
    client = boto3.client(service_name)
    return client
