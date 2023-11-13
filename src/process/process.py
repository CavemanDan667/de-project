import logging
from pg8000.native import Connection
from process_utils.get_credentials import get_credentials
from process_utils.write_to_parquet import write_data_to_parquet
from process_utils.extract_filepath import extract_filepath
from process_utils.extract_event_data import extract_event_data
from process_utils.transform_currency import transform_currency
from process_utils.transform_design import transform_design
from process_utils.transform_payment_type import transform_payment_type
from process_utils.transform_counterparty import transform_counterparty
from process_utils.transform_sales_order import transform_sales_order
from process_utils.transform_staff import transform_staff
from process_utils.transform_address import transform_address

dw_config = get_credentials("data_warehouse_creds")
totesys_config = get_credentials("totesys_database_creds")

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
            password=dw_password,
        )

        totesys_conn = Connection(
            user=totesys_user,
            host=totesys_host,
            database=totesys_database,
            port=totesys_port,
            password=totesys_password,
        )

        table_name, unix = extract_event_data(event)
        file_path = extract_filepath(event)
        data_frame = None
        process_table_name = None

        if table_name == "currency":
            data_frame = transform_currency(file_path)
            process_table_name = f"dim_{table_name}"

        elif table_name == "address":
            data_frame = transform_address(file_path)
            process_table_name = "dim_location"

        elif table_name == "payment_type":
            data_frame = transform_payment_type(file_path)
            process_table_name = f"dim_{table_name}"

        elif table_name == "counterparty":
            for i in range(5):
                try:
                    data_frame = transform_counterparty(file_path, dw_conn)
                    process_table_name = f"dim_{table_name}"
                    break
                except KeyError or ValueError:
                    continue

        elif table_name == "design":
            data_frame = transform_design(file_path)
            process_table_name = f"dim_{table_name}"

        elif table_name == "staff":
            data_frame = transform_staff(file_path, totesys_conn)
            process_table_name = f"dim_{table_name}"

        elif table_name == "sales_order":
            data_frame = transform_sales_order(file_path)
            process_table_name = f"fact_{table_name}"

        elif table_name in [
            "department",
            "payment",
            "purchase_order",
            "transaction"
        ]:
            pass

        else:
            logger.error(f"Table name not recognised: {table_name}")

        if data_frame is not None and process_table_name is not None:
            write_data_to_parquet(unix, process_table_name, data_frame)
        else:
            logger.info(
                f"""{table_name} has been updated but write
                to parquet has not been enabled"""
            )

    except Exception as e:
        logger.error(f"Process handler has raised an error: {e}")
        raise e
