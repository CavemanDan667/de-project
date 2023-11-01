from pg8000.native import DatabaseError, identifier, literal



def fetch_data(conn, table_name, newest_time, time_now):
    """
    This function takes a database connection and a table name,
    along with two timestamps,
    and fetches the data from that table
    updated between those times.

    Args:
        conn: an established connection to a database.
        table_name(string): the name of a table within that database.
        newest_time: the last timestamp found on a previous file.
        time_now: a timestamp created in the parent function.

    Returns:
        If successful, a dictionary
        containing all headers and rows from the table.
        In the case of an error, the function returns error codes.

    Raises:
        TypeError if parameters are missing.
    """
    try:
        query = f"""SELECT * FROM {identifier(table_name)}
        WHERE last_updated BETWEEN {literal(newest_time)}
        AND {literal(time_now)}"""
        data = conn.run(query)
        headers = [c['name'] for c in conn.columns]
        return {'Headers': headers, 'Rows': data}
    except DatabaseError as d:
        print(d)
        return f'There was a database error: {table_name}'

