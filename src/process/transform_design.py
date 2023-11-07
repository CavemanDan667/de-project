import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_design(csv_file, conn):
    data = pd.read_csv(csv_file,
                       usecols=['design_name', 'file_location', 'file_name'])
    for row in data.values.tolist():
        try:
            query = f'''INSERT INTO dim_design
                        (design_name, file_location, file_name)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])});'''
            conn.run(query)
        except DatabaseError as d:
            raise d
    return data
