import pandas as pd
from pg8000.native import DatabaseError, literal

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_staff(parquet_file, conn):
    """This function reads a processed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_staff
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
            select_query = f'''SELECT * FROM dim_staff
            WHERE staff_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_staff (
                    staff_id,
                    first_name,
                    last_name,
                    department_name,
                    location,
                    email_address
                    ) VALUES (
                    {literal(row[0])},
                    {literal(row[1])},
                    {literal(row[2])},
                    {literal(row[3])},
                    {literal(row[4])},
                    {literal(row[5])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE dim_staff
                    SET first_name = {literal(row[1])},
                    last_name = {literal(row[2])},
                    department_name = {literal(row[3])},
                    location = {literal(row[4])},
                    email_address = {literal(row[5])}
                    WHERE staff_id = {literal(row[0])}'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d
        except ValueError as v:
            logger.error(f'Load handler has raised an error: {v}')
            raise v
        except Exception as e:
            raise e
    return 'Data loaded successfully - dim_staff'
