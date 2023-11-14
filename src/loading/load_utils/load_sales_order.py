import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_sales_order(parquet_file, conn):
    """
    This function reads a parquet file of transformed data.
    For each sale, it checks to see if that combination of sales_id,
    updated_time and updated_date exists within the fact_sales_order table.
    If it doesn't, the function inserts the new sales record into the table.

    Args:
        parquet_file: a filepath to a parquet file containing
        data transformed from the original database.
        conn: a connection to the new data warehouse.

     Returns:
        a message confirming successful addition.

    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
        KeyError/IndexError: if the columns in the passed parquet file
        do not match the expected columns.
    """
    sales_order_data = wr.s3.read_parquet(path=parquet_file, columns=[
        'sales_order_id',
        'created_date',
        'created_time',
        'last_updated_date',
        'last_updated_time',
        'sales_staff_id',
        'counterparty_id',
        'units_sold',
        'unit_price',
        'currency_id',
        'design_id',
        'agreed_payment_date',
        'agreed_delivery_date',
        'agreed_delivery_location_id'
    ])
    sales_order_list = sales_order_data.values.tolist()
    if len(sales_order_list) == 0:
        logger.error("load_sales_order was given an incorrect file")
        raise KeyError

    for sale in sales_order_list:
        try:
            select_query = f'''
                    SELECT * FROM fact_sales_order
                    WHERE sales_order_id = {literal(sale[0])}
                    AND last_updated_date = {literal(sale[3])}
                    AND last_updated_time = {literal(sale[4])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                                INSERT INTO fact_sales_order
                                (sales_order_id,
                                created_date,
                                created_time,
                                last_updated_date,
                                last_updated_time,
                                sales_staff_id,
                                counterparty_id,
                                units_sold,
                                unit_price,
                                currency_id,
                                design_id,
                                agreed_payment_date,
                                agreed_delivery_date,
                                agreed_delivery_location_id)
                                VALUES (
                                    {literal(sale[0])},
                                    {literal(sale[1])},
                                    {literal(sale[2])},
                                    {literal(sale[3])},
                                    {literal(sale[4])},
                                    {literal(sale[5])},
                                    {literal(sale[6])},
                                    {literal(sale[7])},
                                    {literal(sale[8])},
                                    {literal(sale[9])},
                                    {literal(sale[10])},
                                    {literal(sale[11])},
                                    {literal(sale[12])},
                                    {literal(sale[13])}
                                );'''
                conn.run(insert_query)
        except IndexError as x:
            logger.error(f"load_sales_order has raised an error: {x}")
            raise x
        except DatabaseError as d:
            logger.error(f"load_sales_order has raised an error: {d}")
            raise d
    return 'Data loaded successfully - fact_sales_order'
