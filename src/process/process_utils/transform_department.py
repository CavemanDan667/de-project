import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_department(csv_file, conn):
    data = pd.read_csv(csv_file,
                       usecols=['department_id',
                                'department_name',
                                'location'])

    for row in data.values.tolist():
        try:
            select_query = f'''SELECT * FROM ref_department
            WHERE department_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO ref_department
                        (department_id, department_name, location)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE ref_department
                SET department_name = {literal(row[1])},
                location = {literal(row[2])}
                WHERE department_id = {literal(row[0])}'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return data
