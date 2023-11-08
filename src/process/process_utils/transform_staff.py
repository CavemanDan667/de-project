import pandas as pd
from pg8000.native import Connection, DatabaseError, literal
from dotenv import dotenv_values

config = dotenv_values(".env")

user = config["TESTDW_USER"]
password = config["TESTDW_PASSWORD"]
host = config["TESTDW_HOST"]
port = config["TESTDW_PORT"]
database = config["TESTDW_DATABASE"]


def conn():
    return Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database
    )


def transform_staff(csv_file, conn):
    staff_data = pd.read_csv(csv_file,
                             usecols=['staff_id',
                                      'first_name',
                                      'last_name',
                                      'department_id',
                                      'email_address'])
    for row in staff_data.values.tolist():
        try:
            department_query = f"""
                SELECT * FROM ref_department
                WHERE department_id = {literal(row[3])};
                """
            result = conn().run(department_query)
            department_dictionary = {
                'department_id': [item[0] for item in result],
                'department_name': [item[1] for item in result],
                'location': [item[2] for item in result]
            }

            department_dataframe = pd.DataFrame.from_dict(
                department_dictionary)

            staff_department_dataframe = pd.merge(
                staff_data, department_dataframe, on='department_id')

            staff_department_dataframe = staff_department_dataframe.drop(
                columns='department_id')

            select_query = f'''
                    SELECT * FROM dim_staff
                    WHERE email_address = {literal(row[4])};'''
            query_result = conn().run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                    INSERT INTO dim_staff
                    (staff_id, first_name, last_name, 
                    email_address, department_name, 
                    location)
                    VALUES
                    ({literal(staff_department_dataframe.values.tolist()[0][0])},
                    {literal(staff_department_dataframe.values.tolist()[0][1])},
                    {literal(staff_department_dataframe.values.tolist()[0][2])},
                    {literal(staff_department_dataframe.values.tolist()[0][3])},
                    {literal(staff_department_dataframe.values.tolist()[0][4])},
                    {literal(staff_department_dataframe.values.tolist()[0][5])});
                    '''
            elif len(query_result) > 0:
                insert_query = f'''
                    UPDATE dim_staff
                    SET first_name = {literal(row[1])},
                    last_name = {literal(row[2])},
                    email_address = {literal(row[4])}
                    WHERE staff_id = {literal(row[0])}
                    ;'''
            conn().run(insert_query)
        except DatabaseError as d:
            raise d
    return staff_department_dataframe


transform_staff(
    '/home/dcox/northcoders/data/de-project/tests/csv_test_files/test-staff.csv', conn)
