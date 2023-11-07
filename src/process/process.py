import os
import logging
from process_utils.write_to_parquet import write_data_to_parquet
from process_utils.extract_filepath import extract_filepath
from process_utils.extract_event_data import extract_event_data

user = os.environ["user"]
host = os.environ["host"]
database = os.environ["database"]
password = os.environ["password"]
port = os.environ["port"]


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    table_name, unix = extract_event_data(event)
    file_path = extract_filepath(event)
    print(file_path)
    data_frame = "some function"
    write_data_to_parquet(unix, table_name, data_frame)
