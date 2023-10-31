import logging
import tempfile
from utils.get_time import get_time
from utils.get_client import get_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Creating a file locally")
    now = get_time()
    print(now)
    print('hi')
    create_text_file()


def create_text_file():
    temp = tempfile.TemporaryFile()
    temp.write(b'This is a test')
    upload_object(temp)


def upload_object(file_name):
    s3 = get_client("s3")
    file_name.seek(0)
    s3.put_object(Bucket="de-project-ingestion-bucket",
                  Key="test_file.txt", Body=file_name)
    logger.info("File created")
    file_name.close()
