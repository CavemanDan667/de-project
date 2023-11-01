from pg8000.native import DatabaseError


def fetch_data(conn, table_name):
    """
    This function takes a database connection and a table name,
    and fetches the data from that table.

    Args:
        conn: an established connection to a database.
        table_name(string): the name of a table within that database.

    Returns:
        If successful, a dictionary
        containing all headers and rows from the table.
        In the case of an error, the function returns error codes.

    Raises:
        TypeError if parameters are missing.
    """
    try:
        data = conn.run(f"SELECT * FROM {table_name}")
        headers = [c['name'] for c in conn.columns]
        return {'Headers': headers, 'Rows': data}
    except DatabaseError:
        return 'Table not found'
