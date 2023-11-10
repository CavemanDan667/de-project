import pandas as pd
import awswrangler as wr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def transform_staff(csv_file, conn_db):
    """This function reads an ingested file of staff data.
    It then runs a query against the department table in the
    original database, returning department_id, name and
    location, which it merges into the new staff data.
    Returns a DataFrame.

    Args:
        csv_file: a filepath to a csv file containing
        data ingested from the original database.
        conn_db: a connection to the original database.
    Returns:
        a dataframe frame containing all of the information
        that has been added to the dim_staff
        table in the new data warehouse.
    Raises:
        KeyError: if the columns in the csv file are
        not as expected.

    """
    try:
        staff_data = wr.s3.read_csv(
            path=csv_file,
            usecols=[
                "staff_id",
                "first_name",
                "last_name",
                "department_id",
                "email_address",
            ],
        )

        department_query = """SELECT department_id,
                            department_name,
                            location
                            FROM department;"""
        department_data = conn_db.run(department_query)
        department_dict = {item[0]: item[1:] for item in department_data}
        staff_list = staff_data.values.tolist()
        staff_dict = {
            "staff_id": [item[0] for item in staff_list],
            "first_name": [item[1] for item in staff_list],
            "last_name": [item[2] for item in staff_list],
            "department_name": [department_dict[item[3]][0]
                                for item in staff_list],
            "location": [department_dict[item[3]][1] for item in staff_list],
            "email_address": [item[4] for item in staff_list],
        }
        staff_frame = pd.DataFrame.from_dict(staff_dict)
    except KeyError as k:
        raise k
    except ValueError as v:
        logger.error(f"Load handler has raised an error: {v}")
        raise v
    return staff_frame
