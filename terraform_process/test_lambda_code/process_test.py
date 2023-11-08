from pg8000.native import Connection
from forex_python.converter import CurrencyCodes
import os
import logging
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv

load_dotenv()

user = os.environ['USER']
host = os.environ['HOST']
database = os.environ["DATABASE"]
password = os.environ["PASSWORD"]
port = os.environ["PORT"]


def handler(event, context):
    print("Hello world")
    conn = Connection(
        user=user, host=host, database=database, port=port, password=password
    )
    print("pg8000 works >>>>>", get_table_names(conn))
    c = CurrencyCodes()
    print("forex-python works >>>>>>", c.get_currency_name("USD"))
    test_df = pd.DataFrame({"A": [1, 2, 3]})
    print("pandas works >>>>>>", test_df)

    df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})

    wr.s3.to_parquet(
        df=df,
        path="s3://test-bucket-de-project/test_file.parquet"
    )

    read_file = wr.s3.read_parquet("s3://test-bucket-de-project/test_file.parquet")
    print('Parquet works', read_file)


def get_table_names(conn):
    """
    This function connects to a database
    and returns a list of all the tables.

    Args:
        conn: A connection set up to a specific database,
        allowing this function to be used on any database.

    Returns:
        A list of all table names within that database,
        if connection is successful.

    Raises:
        TypeError if invoked without a connection.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    try:
        data = conn.run(
            """SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        AND table_type='BASE TABLE'
                        AND table_name NOT LIKE '\\_%';
                        """
        )
        table_list = [item[0] for item in data]
        return table_list
    except TypeError as t:
        logger.error(f"get_tables has raised: {t}")
        raise t
