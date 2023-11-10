import logging
from pg8000.native import Connection
from process_utils.write_to_parquet import write_data_to_parquet
from process_utils.extract_filepath import extract_filepath
from process_utils.extract_event_data import extract_event_data
from process_utils.transform_currency import transform_currency
from process_utils.transform_design import transform_design
from process_utils.get_credentials import get_credentials


dw_config = get_credentials('data_warehouse_creds')
totesys_config = get_credentials('totesys_database_creds')

dw_user = dw_config["DW_USER"]
dw_host = dw_config["DW_HOST"]
dw_database = dw_config["DW_DATABASE"]
dw_password = dw_config["DW_PASSWORD"]
dw_port = dw_config["DW_PORT"]

totesys_user = totesys_config["TOTESYS_USER"]
totesys_host = totesys_config["TOTESYS_HOST"]
totesys_database = totesys_config["TOTESYS_DATABASE"]
totesys_password = totesys_config["TOTESYS_PASSWORD"]
totesys_port = totesys_config["TOTESYS_PORT"]

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
        dw_conn = Connection(
            user=dw_user,
            host=dw_host,
            database=dw_database,
            port=dw_port,
            password=dw_password
        )

        data_frame = None

        table_name, unix = extract_event_data(event)
        file_path = extract_filepath(event)

        if table_name == "currency":
            data_frame = transform_currency(dw_conn, file_path)
        elif table_name == "design":
            data_frame = transform_design(dw_conn, file_path)

        write_data_to_parquet(unix, table_name, data_frame)
    except Exception as e:
        logger.error(f"Process handler has raised an error: {e}")
        raise e
