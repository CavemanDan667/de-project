import csv
from src.fetch_data import fetch_data


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
    fetched_data = fetch_data(table_name)
    write_data_to_csv(fetched_data)


print(fetch_and_write_to_csv(table_name='payment'))
