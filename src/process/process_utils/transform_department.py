import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_department(csv_file, conn):
    """This function reads an ingested file of department data.
    It then checks whether each department_id from this data
    appears in the ref_department table. If the department_id is
    not found in ref_department, this function adds the relevant
    data to ref_department. If department_id is found in ref_department,
    this function updates the relevant record in ref_department.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.
    Returns:
        a data frame containing all of the information
        that has been added to the ref_department
        table in the new data warehouse.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """
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
