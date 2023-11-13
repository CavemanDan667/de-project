import pandas as pd
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_purchase_order(csv_file):
    """
    This function reads an ingested file of purchase data.
    It transforms that data into a re-ordered list, splitting
    the datetimes into separate date and time entries.
    It then converts that list into a dataframe and returns.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.

     Returns:
        a data frame containing the section of the fact_purchase_order table
        to be added to the new data warehouse created by the function.

    Raises:
        IndexError: if the passed .csv file does not contain
        the correct number of columns, or if the datetime input
        does not match the expected length.
    """

    purchase_order_data = wr.s3.read_csv(path=csv_file)

    purchase_order_list = purchase_order_data.values.tolist()
    fact_purchase_order_list = []
    try:
        for purchase in purchase_order_list:
            fact_purchase_order_list.append([purchase[0],
                                             purchase[1][0:10],
                                             purchase[1][11:19],
                                             purchase[2][0:10],
                                             purchase[2][11:19],
                                             purchase[3],
                                             purchase[4],
                                             purchase[5],
                                             purchase[6],
                                             purchase[7],
                                             purchase[8],
                                             purchase[9],
                                             purchase[10],
                                             purchase[11]])
    except IndexError as x:
        logger.error(f"Load handler has raised an error: {x}")
        raise x
    fact_purchase_order_df = pd.DataFrame(fact_purchase_order_list, columns=[
        'purchase_order_id', 'created_date', 'created_time',
        'last_updated_date', 'last_updated_time', 'staff_id',
        'counterparty_id', 'item_code', 'item_quantity',
        'item_unit_price', 'currency_id', 'agreed_payment_date',
        'agreed_delivery_date', 'agreed_delivery_location_id'])
    return fact_purchase_order_df
