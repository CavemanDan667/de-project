import logging
from pg8000.native import Connection
from load_utils.load_address import load_address
from load_utils.load_counterparty import load_counterparty
from load_utils.load_currency import load_currency
from load_utils.load_design import load_design
from load_utils.load_payment_type import load_payment_type
from load_utils.load_sales_order import load_sales_order
from load_utils.load_staff import load_staff
from load_utils.get_credentials import get_credentials
from src.process.process_utils.extract_event_data import extract_event_data
from src.process.process_utils.extract_filepath import extract_filepath

dw_config = get_credentials('data_warehouse_creds')

dw_user = dw_config["DW_USER"]
dw_host = dw_config["DW_HOST"]
dw_database = dw_config["DW_DATABASE"]
dw_password = dw_config["DW_PASSWORD"]
dw_port = dw_config["DW_PORT"]

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    AWS Lambda handler function that calls
    and manages utility functions for loading data
    when parquet files of transformed data
    are created in an s3 bucket.

    Args:
        event (dict): AWS S3 PUT event object
      
    Actions:
        invokes relevant load function, or logs an 
error if given table does not exist

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

        table_name, unix = extract_event_data(event)
        file_path = extract_filepath(event)

        if table_name == "dim_counterparty":
            load_counterparty(file_path, dw_conn)
        if table_name == "dim_currency":
            load_currency(file_path, dw_conn)
        if table_name == "dim_design":
            load_design(file_path, dw_conn)
        if table_name == "dim_location":
            load_address(file_path, dw_conn)
        if table_name == "dim_payment_type":
            load_payment_type(file_path, dw_conn)
        if table_name == "fact_sales_order":
            load_sales_order(file_path, dw_conn)
        if table_name == "dim_staff":
            load_staff(file_path, dw_conn)
        else:
            logger.error(f"Table not recognised: {table_name}")

    except Exception as e:
        logger.error(f"Load handler has raised an error: {e}")
        raise e
