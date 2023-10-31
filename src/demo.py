import logging
import tempfile
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Creating a file locally")
    create_text_file()


def create_text_file():
    temp = tempfile.TemporaryFile()
    temp.write(b'This is a test')
    upload_object(temp)


def get_client(service_name):
    client = boto3.client(service_name)
    return client


def upload_object(file_name):
    s3 = get_client("s3")
    file_name.seek(0)
    s3.put_object(Bucket="de-project-ingestion-bucket",
                  Key="test_file.txt", Body=file_name)
    logger.info("File created")
    file_name.close()
