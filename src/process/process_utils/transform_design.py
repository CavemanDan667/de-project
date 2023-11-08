import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_design(csv_file, conn):
    """This function reads an ingested file of design data.
    It then checks whether each design_id from this data
    appears in the dim_design table. If the design_id is
    not found in dim_design, this function adds the relevant
    data to dim_design. If design_id is found in dim_design,
    this function updates the relevant record in dim_design.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn: a connection to the new data warehouse.
    Returns:
        a data frame containing all of the information
        that has been added to the dim_design
        table in the new data warehouse.
    Raises:
        DatabaseError: if either the select or insert
        query fails to match up to the destination
        table.
    """
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
