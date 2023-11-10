import pandas as pd
from pg8000.native import DatabaseError, literal

def load_counterparty(parquet_file, conn):
    """This function reads a processed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_counterparty
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
            select_query = f'''SELECT * FROM dim_counterparty
            WHERE counterparty_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_counterparty (
                    counterparty_id,
                    counterparty_legal_name,
                    counterparty_legal_address_line_1,
                    counterparty_legal_address_line_2,
                    counterparty_legal_district,
                    counterparty_legal_city,
                    counterparty_legal_postal_code,
                    counterparty_legal_country,
                    counterparty_legal_phone_number
                    ) VALUES (
                    {literal(row[0])},
                    {literal(row[1])},
                    {literal(row[2])},
                    {literal(row[3])},
                    {literal(row[4])},
                    {literal(row[5])},
                    {literal(row[6])},
                    {literal(row[7])},
                    {literal(row[8])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE dim_counterparty
                    SET counterparty_legal_name = {literal(row[1])},
                    counterparty_legal_address_line_1 = {literal(row[2])},
                    counterparty_legal_address_line_2 = {literal(row[3])},
                    counterparty_legal_district = {literal(row[4])},
                    counterparty_legal_city = {literal(row[5])},
                    counterparty_legal_postal_code = {literal(row[6])},
                    counterparty_legal_country = {literal(row[7])},
                    counterparty_legal_phone_number = {literal(row[8])}
                    WHERE counterparty_id = {literal(row[0])}'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d
        except Exception as e:
            raise e
    return 'Data loaded successfully - dim_counterparty'
