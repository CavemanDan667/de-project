import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_staff(staff_csv_file, department_csv_file, conn):
    data_staff = pd.read_csv(staff_csv_file, usecols=['first_name',
                                                      'last_name',
                                                      'email_address',
                                                      'department_id'])
    data_department = pd.read_csv(department_csv_file,
                                  usecols=['department_name',
                                           'location',
                                           'department_id'])
    data_staff_department = pd.merge(data_staff,
                                     data_department, on='department_id')
    data_staff_department = data_staff_department.drop(columns='department_id')

    for row in data_staff_department.values.tolist():
        try:
            query = f'''INSERT INTO dim_staff
                        (first_name,
                        last_name,
                        email_address,
                        department_name,
                        location)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])},
                         {literal(row[2])},
                         {literal(row[3])},
                         {literal(row[4])});'''
            conn.run(query)
        except DatabaseError as d:
            raise d

    return data_staff_department
