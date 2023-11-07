import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_design(csv_file, conn):
    data = pd.read_csv(csv_file,
                       usecols=[
                           'design_id',
                           'design_name',
                           'file_location',
                           'file_name'
                           ])
    for row in data.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_design
            WHERE design_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_design
                        (design_id, design_name, file_location, file_name)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])},
                         {literal(row[3])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE dim_design
                SET design_name = {literal(row[1])},
                file_location = {literal(row[2])},
                file_name = {literal(row[3])}
                WHERE design_id = {literal(row[0])}'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return data
