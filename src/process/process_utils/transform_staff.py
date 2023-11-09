import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_staff(csv_file, conn_db, conn_dw):
    """This function reads an ingested file of staff data.
    It then runs a query against the department table in the
    original database, returning department_id, name and
    location, which it merges into the new staff data.
    It then checks whether each staff_id from the merged data
    appears in the dim_staff table. If the staff_id is
    not found in dim_staff, this function adds the relevant
    data to dim_staff. If staff_id is found in dim_staff,
    this function updates the relevant record in dim_staff.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn_db: a connection to the original database.
        conn_dw: a connection to the new data warehouse.
    Returns:
        a data frame containing all of the information
        that has been added to the dim_staff
        table in the new data warehouse.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """

    staff_data = pd.read_csv(csv_file,
                             usecols=['staff_id',
                                      'first_name',
                                      'last_name',
                                      'department_id',
                                      'email_address'])

    department_query = '''SELECT department_id,
                        department_name,
                        location
                        FROM department;'''
    department_data = conn_db.run(department_query)
    department_dict = {item[0]: item[1:] for item in department_data}
    staff_list = staff_data.values.tolist()
    staff_dict = {
        'staff_id': [item[0] for item in staff_list],
        'first_name': [item[1] for item in staff_list],
        'last_name': [item[2] for item in staff_list],
        'department_name': [
            department_dict[item[3]][0]
            for item in staff_list
        ],
        'location': [department_dict[item[3]][1] for item in staff_list],
        'email_address': [item[4] for item in staff_list]
    }
    staff_frame = pd.DataFrame.from_dict(staff_dict)
    for row in staff_frame.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_staff
            WHERE staff_id = {literal(row[0])};'''
            query_result = conn_dw.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_staff (
                    staff_id,
                    first_name,
                    last_name,
                    department_name,
                    location,
                    email_address
                    ) VALUES (
                    {literal(row[0])},
                    {literal(row[1])},
                    {literal(row[2])},
                    {literal(row[3])},
                    {literal(row[4])},
                    {literal(row[5])});'''
            elif len(query_result) > 0:
                insert_query = f'''UPDATE dim_staff
                    SET first_name = {literal(row[1])},
                    last_name = {literal(row[2])},
                    department_name = {literal(row[3])},
                    location = {literal(row[4])},
                    email_address = {literal(row[5])}
                    WHERE staff_id = {literal(row[0])}'''
            conn_dw.run(insert_query)
        except DatabaseError as d:
            raise d
    return staff_frame
