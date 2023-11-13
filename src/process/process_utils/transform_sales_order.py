import pandas as pd
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_sales_order(csv_file):
    """
    This function reads an ingested file of sales data.
    It transforms that data into a re-ordered list, splitting
    the datetimes into separate date and time entries.
    It then converts that list into a dataframe and returns.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.

     Returns:
        a data frame containing the section of the fact_sales_order table
        to be added to the new data warehouse created by the function.

    Raises:
        IndexError: if the passed .csv file does not contain
        the correct number of columns, or if the datetime input
        does not match the expected length.
    """
    sales_order_data = wr.s3.read_csv(path=csv_file)

    sales_order_list = sales_order_data.values.tolist()
    fact_sales_order_list = []
    try:
        for sale in sales_order_list:
            fact_sales_order_list.append([sale[0],
                                          sale[1][0:10],
                                          sale[1][11:19],
                                          sale[2][0:10],
                                          sale[2][11:19],
                                          sale[4],
                                          sale[5],
                                          sale[6],
                                          sale[7],
                                          sale[8],
                                          sale[3],
                                          sale[9],
                                          sale[10],
                                          sale[11]])
    except IndexError as x:
        logger.error(f"Load handler has raised an error: {x}")
        raise x
    fact_sales_order_df = pd.DataFrame(fact_sales_order_list, columns=['sales_order_id', 'created_date', 'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id'])
    return fact_sales_order_df
