import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging


def load_address(parquet_file, conn):
    """This function reads a transformed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_location
    table with the data from that file.

    Args:
        parquet_file: a filepath to a parquet file containing
        data transformed in a separate function.
        conn: a connection to the new data warehouse.
    Returns:
        a message confirming successful load of data.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
        KeyError/IndexError: if the columns in the passed parquet file
        do not match the expected columns.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    data = wr.s3.read_parquet(
        path=parquet_file, columns=[
            'address_id',
            'address_line_1',
            'address_line_2',
            'district',
            'city',
            'postal_code',
            'country',
            'phone'
        ])
    if len(data.values.tolist()) == 0:
        logger.error("load_address was given an incorrect file")
        raise KeyError
    try:
        for item in data.values.tolist():
            select_query = f"""
                SELECT * FROM dim_location
                WHERE location_id = {literal(item[0])}"""
            select_result = conn.run(select_query)
            if len(select_result) == 0:
                if type(item[2]) is not str:
                    item[2] = None
                if type(item[3]) is not str:
                    item[3] = None
                query = f"""INSERT INTO dim_location (
                        location_id, address_line_1,
                        address_line_2, district,
                        city, postal_code,
                        country, phone
                        ) VALUES (
                        {literal(item[0])}, {literal(item[1])},
                        {literal(item[2])}, {literal(item[3])},
                        {literal(item[4])}, {literal(item[5])},
                        {literal(item[6])}, {literal(item[7])}
                        )"""
                conn.run(query)
        return "Data loaded successfully - dim_location"
    except DatabaseError as d:
        logger.error(f'load_address has raised a database error: {d}')
        raise d
    except IndexError as x:
        logger.error(f"load_address has raised an error: {x}")
        raise x
