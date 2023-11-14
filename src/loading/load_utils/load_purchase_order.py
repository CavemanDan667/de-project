import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_purchase_order(parquet_file, conn):
    """
    This function reads parquet_file of transformed data.
    For each sale, it checks to see if that combination of purchase_id,
    updated_time and updated_date are within the fact_purchase_order table.
    If it isn't, the function inserts the new purchase data into the table,
    whether it is a new purchase, or an update to a previous purchase.

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
    purchase_order_data = wr.s3.read_parquet(path=parquet_file, columns=[
        'purchase_order_id',
        'created_date',
        'created_time',
        'last_updated_date',
        'last_updated_time',
        'staff_id',
        'counterparty_id',
        'item_code',
        'item_quantity',
        'item_unit_price',
        'currency_id',
        'agreed_delivery_date',
        'agreed_payment_date',
        'agreed_delivery_location_id'
    ])
    purchase_order_list = purchase_order_data.values.tolist()
    if len(purchase_order_list) == 0:
        logger.error("load_purchase_order was given an incorrect file")
        raise KeyError
    for purchase in purchase_order_list:
        try:
            select_query = f'''
                    SELECT * FROM fact_purchase_order
                    WHERE purchase_order_id = {literal(purchase[0])}
                    AND last_updated_date = {literal(purchase[3])}
                    AND last_updated_time = {literal(purchase[4])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                                INSERT INTO fact_purchase_order
                                (purchase_order_id,
                                created_date,
                                created_time,
                                last_updated_date,
                                last_updated_time,
                                staff_id,
                                counterparty_id,
                                item_code,
                                item_quantity,
                                item_unit_price,
                                currency_id,
                                agreed_payment_date,
                                agreed_delivery_date,
                                agreed_delivery_location_id)
                                VALUES (
                                    {literal(purchase[0])},
                                    {literal(purchase[1])},
                                    {literal(purchase[2])},
                                    {literal(purchase[3])},
                                    {literal(purchase[4])},
                                    {literal(purchase[5])},
                                    {literal(purchase[6])},
                                    {literal(purchase[7])},
                                    {literal(purchase[8])},
                                    {literal(purchase[9])},
                                    {literal(purchase[10])},
                                    {literal(purchase[11])},
                                    {literal(purchase[12])},
                                    {literal(purchase[13])}
                                );'''
                conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"load_purchase_order has raised an error: {d}")
            raise d
        except IndexError as x:
            logger.error(f"load_purchase_order has raised an error: {x}")
            raise x
    return 'Data loaded successfully - fact_purchase_order'
