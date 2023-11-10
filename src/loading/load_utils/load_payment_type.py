import pandas as pd
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_payment_type(parquet_file, conn):
    """This function reads a processed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_payment_type
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
    data = pd.read_parquet(parquet_file)
    for row in data.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_payment_type
            WHERE payment_type_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_payment_type
                        (payment_type_id, payment_type_name)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])});'''
                conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"Load handler has raised an error: {d}")
            raise d
        except Exception as e:
            logger.error(f"Load handler has raised an error: {e}")
            raise e
    return 'Data loaded successfully - dim_payment_type'
