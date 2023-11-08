import logging
from pg8000.native import Connection
from src.process.process_utils.write_to_parquet import write_data_to_parquet
from src.process.process_utils.extract_filepath import extract_filepath
from src.process.process_utils.extract_event_data import extract_event_data
from src.process.process_utils.transform_currency import transform_currency
from src.process.process_utils.transform_design import transform_design
from dotenv import dotenv_values

config = dotenv_values(".env")

user = config["DW_USER"]
host = config["DW_HOST"]
database = config["DW_DATABASE"]
password = config["DW_PASSWORD"]
port = config["DW_PORT"]


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    AWS Lambda handler function that calls
    and manages utility functions for processing data
    when files are created in an s3 bucket.

    Args:
        event (dict): AWS S3 PUT event object

    Raises:
        e: Any exception that is missed by the
        utility functions should be caught here.
    """
    try:
        conn = Connection(
            user=user,
            host=host,
            database=database,
            port=port,
            password=password
        )

        data_frame = None

        table_name, unix = extract_event_data(event)
        file_path = extract_filepath(event)

        if table_name == "currency":
            data_frame = transform_currency(conn, file_path)
        elif table_name == "design":
            data_frame = transform_design(conn, file_path)

        write_data_to_parquet(unix, table_name, data_frame)
    except Exception as e:
        logger.error(f"Process handler has raised an error: {e}")
        raise e
