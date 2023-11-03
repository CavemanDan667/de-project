import logging


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
        data = conn.run("""SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema='public'
                        AND table_type='BASE TABLE'
                        AND table_name NOT LIKE '\\_%';
                        """)
        table_list = [item[0] for item in data]
        return table_list
    except TypeError as t:
        logger.error(f'get_tables has raised: {t}')
        raise t
