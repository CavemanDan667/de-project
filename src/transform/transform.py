import logging
import time
from pg8000.native import Connection
from transform_utils.get_credentials import get_credentials
from transform_utils.write_to_parquet import write_data_to_parquet
from transform_utils.extract_filepath import extract_filepath
from transform_utils.extract_event_data import extract_event_data
from transform_utils.transform_currency import transform_currency
from transform_utils.transform_design import transform_design
from transform_utils.transform_payment_type import transform_payment_type
from transform_utils.transform_counterparty import transform_counterparty
from transform_utils.transform_sales_order import transform_sales_order
from transform_utils.transform_staff import transform_staff
from transform_utils.transform_address import transform_address
from transform_utils.transform_purchase_order import transform_purchase_order
from transform_utils.transform_transaction import transform_transaction
from transform_utils.transform_payment import transform_payment

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
    An AWS Lambda handler function that calls
    and manages utility functions for transforming data
    when files are ingested into an s3 bucket.

    Calls:
        extract_event_data: to retrieve the table name of the
                            s3 PUT event and the unix timestamp.
        extract_file_path: to retrieve the s3 filepath for the created object.
        transform_currency:
        transform_address:
        transform_payment_type:
        transform_counterparty:
        transform_design:
        transform_address:
        transform_staff:
        transform_sales_order:
        transform_transaction:
        transform_purchase_order:
        transform_payment:
            To transform the newly-ingested .csv file as required, and retrieve
            a dataframe of the transformed data.
        write_data_to_parquet: to convert the passed dataframe to a
                               parquet file and store in a second s3 bucket.

    Args:
        event (dict): An AWS s3 PUT event object.
        context (dict): Required to run as a lambda.

    Raises:
        Exception: Any exception that is missed by the
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
        transform_table_name = None
        time_out = False

        if table_name == "currency":
            data_frame = transform_currency(file_path)
            transform_table_name = f"dim_{table_name}"

        elif table_name == "address":
            data_frame = transform_address(file_path)
            transform_table_name = "dim_location"

        elif table_name == "payment_type":
            data_frame = transform_payment_type(file_path)
            transform_table_name = f"dim_{table_name}"

        elif table_name == "counterparty":
            for i in range(1, 11):
                try:
                    data_frame = transform_counterparty(file_path, dw_conn)
                    transform_table_name = f"dim_{table_name}"
                    break
                except KeyError or ValueError:
                    logger.info(
                        f'{table_name} attempt {i} failed. Retrying...'
                        )
                    continue
            if data_frame is None or transform_table_name is None:
                time_out = True

        elif table_name == "design":
            data_frame = transform_design(file_path)
            transform_table_name = f"dim_{table_name}"

        elif table_name == "staff":
            data_frame = transform_staff(file_path, totesys_conn)
            transform_table_name = f"dim_{table_name}"

        elif table_name == "sales_order":
            data_frame = transform_sales_order(file_path)
            transform_table_name = f"fact_{table_name}"

        elif table_name == "transaction":
            time.sleep(120)
            for i in range(1, 11):
                try:
                    data_frame = transform_transaction(file_path)
                    transform_table_name = f"dim_{table_name}"
                    break
                except Exception:
                    logger.info(
                        f'{table_name} attempt {i} failed. Retrying...'
                        )
                    continue
            if data_frame is None or transform_table_name is None:
                time_out = True

        elif table_name == "purchase_order":
            for i in range(1, 11):
                try:
                    data_frame = transform_purchase_order(file_path)
                    transform_table_name = f"fact_{table_name}"
                    break
                except Exception:
                    logger.info(
                        f'{table_name} attempt {i} failed. Retrying...'
                        )
                    continue
            if data_frame is None or transform_table_name is None:
                time_out = True

        elif table_name == "payment":
            time.sleep(240)
            for i in range(1, 11):
                try:
                    data_frame = transform_payment(file_path)
                    transform_table_name = f"fact_{table_name}"
                    break
                except Exception:
                    logger.info(
                        f'{table_name} attempt {i} failed. Retrying...'
                        )
                    continue
            if data_frame is None or transform_table_name is None:
                time_out = True

        elif table_name in [
            "department"
        ]:
            pass

        else:
            logger.error(f"Table name not recognised: {table_name}")

        if data_frame is not None and transform_table_name is not None:
            write_data_to_parquet(unix, transform_table_name, data_frame)
        elif time_out is True:
            logger.error[f'{table_name} has failed after 10 attempts']
        else:
            logger.info(
                f"""{table_name} has been updated but write
                to parquet has not been enabled"""
            )

    except Exception as e:
        logger.error(f"Transform handler has raised an error: {e}")
        raise e
