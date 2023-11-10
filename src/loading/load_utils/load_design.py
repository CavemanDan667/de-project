import awswrangler as wr
from pg8000.native import DatabaseError, literal
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def load_design(parquet_file, conn):
    """This function reads a processed parquet file.
    It then uses a connection to the data warehouse
    to populate the corresponding dim_design
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
    data = wr.s3.read_parquet(parquet_file)
    for row in data.values.tolist():
        try:
            select_query = f"""SELECT * FROM dim_design
            WHERE design_id = {literal(row[0])};"""
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f"""INSERT INTO dim_design
                        (design_id, design_name, file_location, file_name)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])},
                         {literal(row[3])});"""
            elif len(query_result) > 0:
                insert_query = f"""UPDATE dim_design
                SET design_name = {literal(row[1])},
                file_location = {literal(row[2])},
                file_name = {literal(row[3])}
                WHERE design_id = {literal(row[0])}"""
            conn.run(insert_query)
        except DatabaseError as d:
            logger.error(f"Load handler has raised an error: {d}")
            raise d
        except Exception as e:
            logger.error(f"Load handler has raised an error: {e}")
            raise e
    return "Data loaded successfully - dim_design"
