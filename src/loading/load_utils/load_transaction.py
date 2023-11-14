import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging
import math

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_transaction(parquet_file, conn):
    """This function reads a processed file of transaction data.
    It then uses a data warehouse connection to check whether
    each transaction_id in the data appears in the dim_transaction table.
    If the transaction_id is not found in dim_transaction, this function
    adds the relevant data to dim_transaction.

    Args:
        parquet_file: a filepath to a parquet file containing
        data processed by a separate function.
        conn: a connection to the new data warehouse.
    Returns:
        a message confirming successful insertion of data.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table, or if the parquet file contains null data in the
        wrong columns.
        KeyError/IndexError: if the columns in the passed parquet file
        do not match the expected columns.
    """
    data = wr.s3.read_parquet(path=parquet_file, columns=[
        'transaction_id',
        'transaction_type',
        'sales_order_id',
        'purchase_order_id'
    ])
    if len(data.values.tolist()) == 0:
        logger.error("load_transaction was given an incorrect file")
        raise KeyError
    for row in data.values.tolist():
        try:
            select_query = f"""SELECT * FROM dim_transaction
            WHERE transaction_id = {literal(row[0])};"""
            query_result = conn.run(select_query)

            if len(query_result) == 0:
                if math.isnan(row[2]):
                    row[2] = None
                if math.isnan(row[3]):
                    row[3] = None
                insert_query = f"""INSERT INTO dim_transaction
                        (transaction_id, transaction_type,
                        sales_order_id, purchase_order_id)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])},
                         {literal(row[3])});"""
                conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"load_transaction has raised an error: {d}")
            raise d
        except IndexError as x:
            logger.error(f"load_transaction has raised an error: {x}")
            raise x
    return "Data loaded successfully - dim_transaction"
