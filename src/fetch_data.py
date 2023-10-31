from pg8000.native import InterfaceError, DatabaseError
import csv
from connection import get_connection


def fetch_data(table_name):
    """
    This function takes a string identifying the table name,
    invokes get_connection to establish a connection to the
    database. It then fetches the data from that table,
    returning the columns and rows.

    In the case of an error, the function returns error codes
    to the user.

    Then, the function closes the connection to the database.
    """
    try:
        con = get_connection()
        data = con.run(f"SELECT * FROM {table_name}")
        headers = [c['name'] for c in con.columns]
        return {'Headers': headers, 'Rows': data}
    except (InterfaceError, DatabaseError) as d:
        print(f'There was a pg8000 error: {d}')
    except Exception as e:
        print(f'There was an unexpected error: {e}')
    finally:
        con.close()


def write_data_to_csv(dictionary):
    """
    This function takes a dictionary as an arguement, which
    is passed from the response of fetch_data; a dictionary
    of headers and rows.

    The function then extracts data from the passed in dictionary
    and writes it in a csv file at the provided filepath.
    """
    filepath = 'data.csv'
    csvfile = open(filepath, 'w', newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(dictionary['Headers'])
    csv_writer.writerows(dictionary['Rows'])


def fetch_and_write_to_csv(table_name):
    """
    This function invokes the two previous functions by
    combining data extractions and writing a CSV file
    """
    fecthed_data = fetch_data(table_name)
    write_data_to_csv(fecthed_data)


print(fetch_and_write_to_csv(table_name='payment'))
