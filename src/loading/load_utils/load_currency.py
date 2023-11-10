import pandas as pd
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_currency(parquet_file, conn):
    """This function reads a processed file of currency data.
    It then uses a data warehouse connection to check whether
    each currency_id in the data appears in the dim_currency table.
    If the currency_id is not found in dim_currency, this function
    adds the relevant data to dim_currency.

    Args:
        parquet_file: a filepath to a parquet file containing
        data processed by a separate function.
        conn: a connection to the new data warehouse.
    Returns:
        a message confirming successful insertion of data.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table, or if the parquet file contains null data.
    """
    try:
        data = pd.read_parquet(parquet_file)
        for value in data.values.tolist():
            select_query = f'''SELECT * FROM dim_currency
            WHERE currency_code = {literal(value[1])};'''
            result = conn.run(select_query)
            if len(result) == 0:
                insert_query = f'''INSERT INTO dim_currency
                (currency_id, currency_code, currency_name)
                VALUES (
                    {literal(value[0])},
                    {literal(value[1])},
                    {literal(value[2])}
                );'''
                conn.run(insert_query)
    except DatabaseError as d:
        logger.error(f"Load handler has raised an error: {d}")
        raise d
    return 'Data loaded successfully - dim_currency'
