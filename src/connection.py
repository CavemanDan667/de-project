from dotenv import dotenv_values
from pg8000.native import Connection, InterfaceError, DatabaseError
import csv
import datetime



config = dotenv_values(".env")

def get_connection():
    """This function connects to the PSQL totesys database using pg8000"""
    return Connection(
        user=config["USER"],
        password=config["PASSWORD"],
        host=config["HOST"],
        port=config["PORT"],
        database=config["DATABASE"],
    )

def fetch_data(table_name):
    """"This function fetches the data by returning the columns and rows from the specified table name"""
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
    """"This function extracts data from the passed in dictionary and writes it in a csv file at the provided filepath"""
    filepath = 'data.csv'
    csvfile = open(filepath, 'w', newline="")
    csv_writer = csv.writer(csvfile)
    csv_writer.writerow(dictionary['Headers'])
    csv_writer.writerows(dictionary['Rows'])

def fetch_and_write_to_csv(table_name):
    """"This function invokes the two previous functions by combining data extractiona and writing a CSV file"""
    fecthed_data = fetch_data(table_name)
    write_data_to_csv(fecthed_data)

print(fetch_and_write_to_csv(table_name='payment'))
