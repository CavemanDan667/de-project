import pandas as pd
from pg8000.native import DatabaseError, literal


def transform_payment_type(csv_file, conn):
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
                           'payment_type_id',
                           'payment_type_name'
                           ])
    for row in data.values.tolist():
        try:
            select_query = f'''SELECT * FROM dim_payment_type
            WHERE payment_type_id = {literal(row[0])};'''
            query_result = conn.run(select_query)
            if len(query_result) == 0:
                insert_query = f'''INSERT INTO dim_payment_type
                        (payment_type_id, payment_type_name)
                        VALUES
                        ({literal(row[0])},
                         {literal(row[1])});'''
                conn.run(insert_query)
        except DatabaseError as d:
            raise d
    return data
