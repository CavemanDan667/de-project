import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging
import math

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_transaction(parquet_file, conn):
    """This function reads a processed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_transaction
    table with the data from that file.

    Args:
        parquet_file: a filepath to a parquet file containing
        data processed in a separate function.
        conn: a connection to the new data warehouse.
    Returns:
        a message confirming successful load of data.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
        Exception: if an unexpected error occurs.
    """
    data = wr.s3.read_parquet(path=parquet_file)
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
                print(row)
                insert_query = f"""INSERT INTO dim_transaction
                        (transaction_id, transaction_type, sales_order_id, purchase_order_id)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])},
                         {literal(row[3])});"""
                conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"Load handler has raised an error: {d}")
            raise d
        except Exception as e:
            logger.error(f"Load handler has raised an error: {e}")
            raise e
    return "Data loaded successfully - dim_transaction"
