import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_staff(csv_file, conn):
    staff_data = pd.read_csv(csv_file,
                             usecols=['staff_id',
                                      'first_name',
                                      'last_name',
                                      'department_id',
                                      'email_address'])

    department_query = 'SELECT * FROM ref_department;'
    department_data = conn.run(department_query)

    department_dataframe = pd.DataFrame(department_data, columns=[
                                        'department_id',
                                        'department_name',
                                        'location'])
    staff_department_dataframe = pd.merge(
        staff_data, department_dataframe, on='department_id')

    staff_department_dataframe = staff_department_dataframe.drop(
        columns='department_id')

    for index, row in enumerate(staff_data.values.tolist()):
        try:
            select_query = f'''
                    SELECT * FROM dim_staff
                    WHERE staff_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''
                    INSERT INTO dim_staff
                    (staff_id, first_name, last_name,
                    email_address, department_name,
                    location)
                    VALUES
                    ({literal(staff_department_dataframe.values.tolist()[index][0])},
                    {literal(staff_department_dataframe.values.tolist()[index][1])},
                    {literal(staff_department_dataframe.values.tolist()[index][2])},
                    {literal(staff_department_dataframe.values.tolist()[index][3])},
                    {literal(staff_department_dataframe.values.tolist()[index][4])},
                    {literal(staff_department_dataframe.values.tolist()[index][5])});
                    '''
            elif len(query_result) > 0:
                insert_query = f'''
                    UPDATE dim_staff
                    SET first_name = {literal(row[1])},
                    last_name = {literal(row[2])},
                    email_address = {literal(row[4])},
                    department_name = {literal(
                        staff_department_dataframe.values.tolist()[index][4])},
                    location = {literal(
                        staff_department_dataframe.values.tolist()[index][5])}
                    WHERE staff_id = {literal(row[0])}
                    ;'''
            conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return staff_department_dataframe
